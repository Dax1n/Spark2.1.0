/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Failure

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.ExecutorAllocationClient
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.python.PythonDStream
import org.apache.spark.streaming.ui.UIUtils
import org.apache.spark.util.{EventLoop, ThreadUtils}


private[scheduler] sealed trait JobSchedulerEvent

private[scheduler] case class JobStarted(job: Job, startTime: Long) extends JobSchedulerEvent

private[scheduler] case class JobCompleted(job: Job, completedTime: Long) extends JobSchedulerEvent

private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent

/**
  * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
  * the jobs and runs them using a thread pool.
  * <br><br>JobScheduler调度流作业在spark上运行(类似于Saprk批处理的DAGScheduler)，使用JobGenerator产生job并在线程池中运行<br><br><br><br><br><br>
  * 持有如下重点成员：<br>
  * 1：JobGenerator
  * 2：InputInfoTracker
  * 3：receiverTracker
  * 4：其他
  */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  // Use of ConcurrentHashMap.keySet later causes an odd runtime problem due to Java 7/8 diff
  // https://gist.github.com/AlainODea/1375759b8720a3f9f094
  /**jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]*/
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]

  /**控制job提交并发度(提交提交job的线程池的线程数目)，默认是1，即它只能一个一个提job*/
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)

  /**
    * jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")<br><br>负责提交作业的线程池
    */
  private val jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")


  /**
    * jobGenerator = new JobGenerator(this)<br><br>
    *   jobGenerator间接的持有‘JobScheduler所有成员的引用’ (例如：inputInfoTracker的引用)
    */
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock

  val listenerBus = new StreamingListenerBus(ssc.sparkContext.listenerBus)

  /**
    * These two are created only when scheduler starts. eventLoop not being null means the scheduler has been started and not stopped
    */
  var receiverTracker: ReceiverTracker = null

  /**
    * A tracker to track all the input stream information as well as processed record number
    * <br><br>重点成员，在Driver端负责DStream资源的监督
    */
  var inputInfoTracker: InputInfoTracker = null

  private var executorAllocationManager: Option[ExecutorAllocationManager] = None

  /**
    * 负责job的启动和异常信息处理的时间循环处理器（其实现内部就是一个线程）
    */
  private var eventLoop: EventLoop[JobSchedulerEvent] = null

  /**
    * org.apache.spark.streaming.scheduler.#start()
    */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      //TODO JobScheduler的重点方法，负责job的启动和异常信息处理
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    eventLoop.start()

    // attach rate controllers of input streams to receive batch completion updates
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)

    listenerBus.start()
    //TODO ReceiverTracker初始化
    receiverTracker = new ReceiverTracker(ssc)
    //TODO 主要用来统计输入数据集的统计信息，用来进行UI显示和监控
    inputInfoTracker = new InputInfoTracker(ssc)

    val executorAllocClient: ExecutorAllocationClient = ssc.sparkContext.schedulerBackend match {
      case b: ExecutorAllocationClient => b.asInstanceOf[ExecutorAllocationClient]
      case _ => null
    }

    executorAllocationManager = ExecutorAllocationManager.createIfEnabled(
      executorAllocClient,
      receiverTracker,
      ssc.conf,
      ssc.graph.batchDuration.milliseconds,
      clock)
    executorAllocationManager.foreach(ssc.addStreamingListener)

    //TODO 重点：ReceiverTracker启动(完成ReceiverSupvisor的创建一起Receiver的启动)
    receiverTracker.start() //start方法中会判断是否为Receiver类型计算源而具体判断是否启动Receiver

    //TODO jobGenerator启动
    jobGenerator.start()

    executorAllocationManager.foreach(_.start())
    logInfo("Started JobScheduler")
  } //TODO JobScheduler的start方法定义结束

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")

    if (receiverTracker != null) {
      // First, stop receiving
      receiverTracker.stop(processAllReceivedData)
    }

    if (executorAllocationManager != null) {
      executorAllocationManager.foreach(_.stop())
    }

    // Second, stop generating jobs. If it has to process all received data,
    // then this will wait for all the processing through JobScheduler to be over.
    jobGenerator.stop(processAllReceivedData)

    // Stop the executor for receiving new jobs
    logDebug("Stopping job executor")
    jobExecutor.shutdown()

    // Wait for the queued jobs to complete if indicated
    val terminated = if (processAllReceivedData) {
      jobExecutor.awaitTermination(1, TimeUnit.HOURS) // just a very large period of time
    } else {
      jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
    }
    if (!terminated) {
      jobExecutor.shutdownNow()
    }
    logDebug("Stopped job executor")

    // Stop everything else
    listenerBus.stop()
    eventLoop.stop()
    eventLoop = null
    logInfo("Stopped JobScheduler")
  }// stop end

  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
      jobSets.put(jobSet.time, jobSet)
      //TODO 线程池jobExecutor执行JobHandler任务（JobHandler是提交job的任务）
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
  }

  def getPendingTimes(): Seq[Time] = {
    jobSets.asScala.keys.toSeq
  }

  def reportError(msg: String, e: Throwable) {
    eventLoop.post(ErrorReported(msg, e))
  }

  def isStarted(): Boolean = synchronized {
    eventLoop != null
  }

  /**
    * 处理三类时间：<br><br>
    * case JobStarted(job, startTime) => handleJobStart(job, startTime)<br><br>
    * case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)<br><br>
    * case ErrorReported(m, e) => handleError(m, e)<br><br>
    *
    * @param event
    */
  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        //
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  private def handleJobStart(job: Job, startTime: Long) {
    val jobSet = jobSets.get(job.time)
    val isFirstJobOfJobSet = !jobSet.hasStarted
    jobSet.handleJobStart(job)
    if (isFirstJobOfJobSet) {
      // "StreamingListenerBatchStarted" should be posted after calling "handleJobStart" to get the
      // correct "jobSet.processingStartTime".
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    job.setStartTime(startTime)
    listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
  }

  private def handleJobCompletion(job: Job, completedTime: Long) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    job.setEndTime(completedTime)
    listenerBus.post(StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      jobGenerator.onBatchCompletion(jobSet.time)
      logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
        jobSet.totalDelay / 1000.0, jobSet.time.toString,
        jobSet.processingDelay / 1000.0
      ))
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
    }
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
    }
  }

  private def handleError(msg: String, e: Throwable) {
    logError(msg, e)
    ssc.waiter.notifyError(e)
    PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
  }

  /**实现Runnable接口的方法，负责提交job作业*/
  private class JobHandler(job: Job) extends Runnable with Logging {

    import JobScheduler._

    def run() {
      val oldProps = ssc.sparkContext.getLocalProperties
      try {
        ssc.sparkContext.setLocalProperties(SerializationUtils.clone(ssc.savedProperties.get()))
        val formattedTime = UIUtils.formatBatchTime(
          job.time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
        val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
        val batchLinkText = s"[output operation ${job.outputOpId}, batch time ${formattedTime}]"

        ssc.sc.setJobDescription(
          s"""Streaming job from <a href="$batchUrl">$batchLinkText</a>""")
        ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString)
        ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString)
        // Checkpoint all RDDs marked for checkpointing to ensure their lineages are
        // truncated periodically. Otherwise, we may run into stack overflows (SPARK-6847).
        ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")

        // We need to assign `eventLoop` to a temp variable. Otherwise, because
        // `JobScheduler.stop(false)` may set `eventLoop` to null when this method is running, then
        // it's possible that when `post` is called, `eventLoop` happens to null.
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          //TODO 发送消息start作业，并没有实际触发作业
          _eventLoop.post(JobStarted(job, clock.getTimeMillis()))
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details.
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            //TODO 真正触发job到集群执行，Job封装计算逻辑
            job.run()
          }
          _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
          }
        } else {
          // JobScheduler has been stopped.
        }
      } finally {
        ssc.sparkContext.setLocalProperties(oldProps)
      }
    }//run方法定义结束
  }

}

private[streaming] object JobScheduler {
  val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime"
  val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId"
}
