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

package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
  * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
  * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
  * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
  * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
  * tasks that can run right away based on the data that's already on the cluster (e.g. map output
  * files from previous stages), though it may fail if this data becomes unavailable.
  * <br><br>
  * 高层次面向stage的调度，为每一个job划分stage，跟踪每一个RDD和stage输出的物化，并且找到一种最小代价调度作业
  * 然后提交stage作为TaskSet给底层的TaskScheduler实现者去在集群中运行他们。一个TaskSet保存独立的任务可以基于集群的数据
  * （或上一个map的输出）立即执行，但是数据如果成为不可获得的话，那么task将会失败
  * <br><br>
  *
  * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
  * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
  * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
  * set of map output files, and another to read those files after a barrier). In the end, every
  * stage will have only shuffle dependencies on other stages, and may compute multiple operations
  * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
  * various RDDs (MappedRDD, FilteredRDD, etc).<br><br>
  * Spark stage的划分依据shuffle的界限。RDD的操作的窄依赖，例如 map filter将会放在一个task中(言外之意 map filter不是划分依据)
  * 但是shuffle动作会划分stage的。最后划分玩stage之后，只有不同的stage之间存在shuffle，stage内部不允许存在shuffle（言外之意就是遇见shuffle就切分stage）
  * 每一个stage可能有多个操作（例如一个stage中有多个map），这些操作组成的流水线当RDD调用compute时候会被执行
  * <br><br>
  *
  * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
  * locations to run each task on, based on the current cache status, and passes these to the
  * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
  * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
  * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
  * a small number of times before cancelling the whole stage.
  *
  * <br><br>  除了切分stage之外，DAGScheduler还会依据当前的cache状态尽可能的locations执行task，传递这些状态
  * 低层的TaskScheduler。DAGScheduler还会处理shuffle数据文件丢失的错误，这种情况丢失数据的stage需要再次提交，
  * 如果一个stage的错误不是由于文件丢失的话将会由TaskScheduler处理，TaskScheduler将会在取消该整个stage之前
  * 重试少数几次每一个task<br>
  *
  * When looking through this code, there are several key concepts:<br>
  * 我们浏览如下代码，有一个关键概念：<br>
  * - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
  * For example, when the user calls an action, like count(), a job will be submitted through
  * submitJob. Each Job may require the execution of multiple stages to build intermediate data.
  * <br>	 Jobs是顶层的单元提交给scheduler，例如：当用户调用action(count())，一个job将会使用submitJob提交
  * 每一个Job可能需要多个stages构建中间数据<br><br>
  *
  * - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
  * task computes the same function on partitions of the same RDD. Stages are separated at shuffle
  * boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
  * fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
  * executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
  * Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
  * <br>
  * Stage是Task的集合用来在Job中计算中间数据，每一个task在同一个RDD的多个分区上执行相同的计算逻辑
  * stage是根据shuffle界限划分的，这将引入一个界限(后面的stage必须等待前面的stage执行完毕去拉取数据)。
  * 这里有两种类型的Stage：ResultStage是执行Action的最终stage，ShuffleMapStage是map为shuffle写输出文件的stage
  * Stage通常可以被多个job共享，如果这些Job都是在相同的RDD上执行计算
  * <br>
  *
  * - Tasks are individual units of work, each sent to one machine.
  * <br>Task是work的独有的单元，每一个Task都会发送到一台机器上<br>
  *
  * - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
  * and likewise remembers which shuffle map stages have already produced output files to avoid
  * redoing the map side of a shuffle.
  * <br>
  * 缓存跟踪：DAGScheduler会解决哪些RDD被缓存，避免重新计算和 避免执行shuffle输出的Satge再次执行<br>
  *
  * - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
  * on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
  * <br>
  * 尽可能本地化：DAGScheduler将会依据RDD所有的位置偏好计算在哪里执行每一个stage中的task，或者在哪里缓存或者输出shuffle data
  * <br><br>
  * - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
  * to prevent memory leaks in a long-running application.
  * <br>清除工作：当job依赖的数据使用完毕之后将会清除这些数据，避免app长期运行导致内存泄露<br><br>
  *
  * To recover from failures, the same stage might need to run multiple times, which are called
  * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
  * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
  * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
  * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
  * stage(s) that compute the missing tasks. As part of this process, we might also have to create
  * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
  * tasks from the old attempt of a stage could still be running, care must be taken to map any
  * events received in the correct Stage object.
  * <br><br>  为了失败恢复，相同的stage可能需要执行多次，这称为"attempts"。如果TaskScheduler汇报一个task由于上一个map输出
  * 文件丢失的话，DAGScheduler将会重新提交丢失的stage，这个可以通过完成的FetchFailed或ExecutorLost时间侦查到。
  * DAGScheduler会等一小段时间去检查是否其他节点也存在这种情况或者是Task失败，然后重新提交TaskSet给丢失的stage计算
  * 丢失的Task。在这个过程中，我们必须从新创建Stage Object，因为之前的Stage Object对象已经被清除。失败的stage有可能由于attempt
  * 还会执行，必须在正确的Stage Object中考虑事件(翻译的不是很得体)<br><br>
  *
  * Here's a checklist to use when making or reviewing changes to this class:
  *
  * - All data structures should be cleared when the jobs involving them end to avoid indefinite
  * accumulation of state in long-running programs.<br>
  * 当job执行完毕之后该job的所有的数据必须清除掉，避免无限的积累<br>
  * - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
  * include the new structure. This will help to catch memory leaks.
  */
private[spark] class DAGScheduler(
                                   private[scheduler] val sc: SparkContext,
                                   private[scheduler] val taskScheduler: TaskScheduler,
                                   listenerBus: LiveListenerBus,
                                   mapOutputTracker: MapOutputTrackerMaster,
                                   blockManagerMaster: BlockManagerMaster,
                                   env: SparkEnv,
                                   clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)

  private[scheduler] def numTotalJobs: Int = nextJobId.get()

  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
    * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
    * that dependency. Only includes stages that are part of currently running job (when the job(s)
    * that require the shuffle stage complete, the mapping will be removed, and the only record of
    * the shuffle data will be in the MapOutputTracker).
    * <br>shuffle dependency id和ShuffleMapStage的映射
    * shuffle dependency id是由SaprkContext产生的，全局位置的id<br>
    *
    */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  /**
    * Stages we need to run whose parents aren't done<br>存储其父stage还没有提交的stage<br>
    */
  private[scheduler] val waitingStages = new HashSet[Stage]

  /**
    * Stages we are running right now<br>需要执行的stage<br>
    */
  private[scheduler] val runningStages = new HashSet[Stage]

  /**
    * Stages that must be resubmitted due to fetch failures
    * <br>由于数据获取失败，必须需要再次提交的Stage<br>
    */
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
    * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
    * and its values are arrays indexed by partition numbers. Each array value is the set of
    * locations where that RDD partition is cached.
    *
    * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
    */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private[scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  /**
    * A closure serializer that we reuse.
    * This is only safe because DAGScheduler runs in a single thread.
    */
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem.
    *
    */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  /**
    * 重点啊，这个DAGScheduler的事件处理器，很多消息发给它，由它处理。包括作业的提交
    */
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  /**
    * Called by the TaskSetManager to report task's starting.
    */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
    * Called by the TaskSetManager to report that a task has completed
    * and results are being fetched remotely.
    */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
    * Called by the TaskSetManager to report task completions or failures.
    */
  def taskEnded(
                 task: Task[_],
                 reason: TaskEndReason,
                 result: Any,
                 accumUpdates: Seq[AccumulatorV2[_, _]],
                 taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
    * Update metrics for in-progress tasks and let the master know that the BlockManager is still
    * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
    * indicating that the block manager should re-register.
    */
  def executorHeartbeatReceived(
                                 execId: String,
                                 // (taskId, stageId, stageAttemptId, accumUpdates)
                                 accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
                                 blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
    * Called by TaskScheduler implementation when an executor fails.
    */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
    * Called by TaskScheduler implementation when a host is added.
    */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
    * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
    * cancellation of the job itself.
    */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
    * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
    * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
    * addition to any missing ancestor shuffle map stages.<br>
    * 如果shuffleIdToMapStage已经存在该依赖的stage的话，则返回该stage，否则根据当前的依赖创建Stage
    * <br>
    *
    */
  private def getOrCreateShuffleMapStage(
                                          shuffleDep: ShuffleDependency[_, _, _],
                                          firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None => //当前依赖不存在Stage，需要创建
        // Create stages for all missing ancestor shuffle dependencies.
        //getMissingAncestorShuffleDependencies(shuffleDep.rdd) 获取爷爷辈的rdd的依赖
        //TODO getMissingAncestorShuffleDependencies(shuffleDep.rdd)查找当前依赖的父RDD的Shuffle依赖栈，栈顶是最前面的shuffle依赖
        //TODO getMissingAncestorShuffleDependencies底层寻找Shuffle依赖也是调用的 getShuffleDependencies
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep => //foreach遍历Shuffle依赖栈
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          //TODO Stage
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            //判断当前的依赖是否创建了Stage，没有创建的话创建
            createShuffleMapStage(dep, firstJobId) //TODO 创建当前的shuffleDep前面的Shuffle依赖的Stage
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId) //创建当前的shuffleDep的Stage
    }
  }

  /**
    * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
    * previously run stage generated the same shuffle data, this function will copy the output
    * locations that are still available from the previous shuffle to avoid unnecessarily
    * regenerating data.<br>创建一个ShuffleMapStage产生给定的shuffle依赖分区，如果前面产生过这次需要的
    * 数据了就不会在重新计算产生数据了，而是复制前面的数据<br>
    *
    */
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // A previously run stage generated partitions for this shuffle, so for each output
      // that's still available, copy information about that output location to the new stage
      // (so we don't unnecessarily re-compute that data).
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
    * * Create a ResultStage associated with the provided jobId.<br>根据提供的jobId创建一个ResultStage<br>
    * 在Spark中只有只有两种Stage分别是：
    * 1：ShuffleMapStage
    * 2：ResultStage
    * <br><br>
    *
    * @param rdd
    * @param func
    * @param partitions
    * @param jobId
    * @param callSite
    * @return resultStage，ResultStage中有 根据当前rdd划分的stage的List[Stage]的引用
    */
  private def createResultStage(
                                 rdd: RDD[_],
                                 func: (TaskContext, Iterator[_]) => _,
                                 partitions: Array[Int],
                                 jobId: Int,
                                 callSite: CallSite): ResultStage = {

    val parents = getOrCreateParentStages(rdd, jobId) //TODO 划分调用，返回一个List[Stage],注意是List[Stage]
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
    * Get or create the list of parent stages for a given RDD.  The new Stages will be created with the provided firstJobId.
    * <br>对于给定的RDD获取或者创建其父Stage的列表<br>在getOrCreateParentStages方法中调用了划分Shuffle以来方法<br>
    * 然后在根据依赖划分Stage
    */
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    //TODO getShuffleDependencies划分调用，返回一个HashSet[ShuffleDependency]，完成依赖的切分，注意返回的HashSet[ShuffleDependency]
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId) // TODO 开始根据每一个依赖创建Satge了
    }.toList
  }

  /**
    * Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet
    * 查找至今没有在shuffleToMapStage中注册的祖先shuffle依赖
    * <br><br>根据当前rdd找到前面所有没有创建stage的shuffle依赖
    *
    * @param rdd
    * @return Stack[ShuffleDependency] 栈顶是最前面的Shuffle依赖
    */
  private def getMissingAncestorShuffleDependencies(
                                                     rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val ancestors = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            //TODO 过滤掉创建Stage的Shuffle依赖
            //TODO 存储当前找到的Shuffle依赖
            ancestors.push(shuffleDep)
            //TODO 将该shfulle依赖的父RDD装入waitingForVisit中，继续向前寻找shuffle依赖，最终找到"传入rdd"前面所有没创建过Stage的shuffle依赖
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
    * Returns shuffle dependencies that are immediate parents of the given RDD.<br>
    * 返回给定的RDD的shuffle依赖，这个函数只会返回上一级的shuffle依赖，不会返回再上一层次的了<br>
    * 例如：A <-- B <-- C  ， C只会返回B，而不会返回A
    * <br>
    *
    * This function will not return more distant ancestors.  For example, if C has a shuffle
    * dependency on B which has a shuffle dependency on A:
    *
    * A <-- B <-- C
    *
    * calling this function with rdd C will only return the B <-- C dependency.
    *
    * This function is scheduler-visible for the purpose of unit testing.
    * <br><br>
    * 重点：切断shuffle依赖的重点方法
    */
  private[scheduler] def getShuffleDependencies(//TODO 划分Stage的方法
                                                rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]] //TODO 存储依赖的
    val visited = new HashSet[RDD[_]] //存储访问过的RDD
    val waitingForVisit = new Stack[RDD[_]] //存储待访问的RDD栈
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      //还有待访问的RDD的话进行访问
      val toVisit = waitingForVisit.pop() //出栈
      if (!visited(toVisit)) {
        //判断当前RDD是否被访问过
        visited += toVisit //存储未访问过的RDD到访问过的set中
        //TODO Spark中就两种依赖，分别是ShuffleDependency和NarrowDependency
        toVisit.dependencies.foreach {
          //toVisit的父RDD可能是一个(例如：map算子)或者多个(例如：UnionRDD)，对toVisit的父RDD进行foreach判断
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep //TODO Shuffle依赖则切分，但是注意啊，容易迷惑，此处支持划分依赖但是还没创建Stage
          case dependency =>
            waitingForVisit.push(dependency.rdd) //记录当前未访问过的RDD
        }
      }
    }
    parents //返回依赖
  }

  /**
    * 根据当前stage寻找父stage，返回stage列表
    *
    * @param stage
    * @return
    */
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
    * Registers the given jobId among the jobs that need the given stage and
    * all of that stage's ancestors.
    * <br><br>
    */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {

    @tailrec //尾递归注解
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter {
          !_.jobIds.contains(jobId)
        }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    } //updateJobIdStageIdMapsList函数定义结束

    updateJobIdStageIdMapsList(List(stage))
  }

  /**
    * Removes state for job and any stages that are not needed by any other job.  Does not
    * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
    *
    * @param job The job whose state to cleanup.
    */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
                .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) {
              // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
    * Submit an action job to the scheduler.
    *
    * @param rdd           target RDD to run tasks on
    * @param func          a function to run on each partition of the RDD
    * @param partitions    set of partitions to run on; some jobs may not want to compute on all
    *                      partitions of the target RDD, e.g. for operations like first()
    * @param callSite      where in the user program this job was called
    * @param resultHandler callback to pass each result to
    * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
    * @return a JobWaiter object that can be used to block until the job finishes executing
    *         or can be used to cancel the job.
    * @throws IllegalArgumentException when partitions ids are illegal
    */
  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       callSite: CallSite,
                       resultHandler: (Int, U) => Unit,
                       properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    //TODO 重点代码，给自己发送消息之后，在org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive进行提交作业
    eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions.toArray, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
    * Run an action job on the given RDD and pass all the results to the resultHandler function as
    * they arrive.<br><br>在给定的RDD上运行一个action触发的作业，传递到来的结果给resultHandler函数<br>
    *
    * @param rdd           target RDD to run tasks on 需要计算的RDD
    * @param func          a function to run on each partition of the RDD 运行在RDD分区上的计算逻辑
    * @param partitions    set of partitions to run on; some jobs may not want to compute on all
    *                      partitions of the target RDD, e.g. for operations like first()
    * @param callSite      where in the user program this job was called
    * @param resultHandler callback to pass each result to
    * @param properties    scheduler properties to attach to this job, e.g. fair scheduler pool name
    * @throws Exception when the job fails
    */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    callSite: CallSite,
                    resultHandler: (Int, U) => Unit,
                    properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
    // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
    // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
    // safe to pass in null here. For more detail, see SPARK-13747.
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
    * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    * as they arrive. Returns a partial result object from the evaluator.
    *
    * @param rdd        target RDD to run tasks on
    * @param func       a function to run on each partition of the RDD
    * @param evaluator  [[ApproximateEvaluator]] to receive the partial results
    * @param callSite   where in the user program this job was called
    * @param timeout    maximum time to wait for the job, in milliseconds
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    */
  def runApproximateJob[T, U, R](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  evaluator: ApproximateEvaluator[U, R],
                                  callSite: CallSite,
                                  timeout: Long,
                                  properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult() // Will throw an exception if the job fails
  }

  /**
    * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
    * can be used to block until the job finishes executing or can be used to cancel the job.
    * This method is used for adaptive query planning, to run map stages and look at statistics
    * about their outputs before submitting downstream stages.
    *
    * @param dependency the ShuffleDependency to run a map stage for
    * @param callback   function called with the result of the job, which in this case will be a
    *                   single MapOutputStatistics object showing how much data was produced for each partition
    * @param callSite   where in the user program this job was submitted
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    */
  def submitMapStage[K, V, C](
                               dependency: ShuffleDependency[K, V, C],
                               callback: MapOutputStatistics => Unit,
                               callSite: CallSite,
                               properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
    * Cancel a job that is running or waiting in the queue.
    */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  /**
    * Cancel all jobs in the given job group ID.
    */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
    * Cancel all jobs that are running or waiting in the queue.
    */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
    * Cancel all jobs associated with a running or scheduled stage.
    */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
    * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
    * the last fetch failure.
    */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
    * Check for waiting stages which are now eligible for resubmission.
    * Submits stages that depend on the given parent stage. Called when the parent stage completes
    * successfully.
    */
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleTaskSetFailed(
                                              taskSet: TaskSet,
                                              reason: String,
                                              exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach {
      abortStage(_, reason, exception)
    }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  /**
    * 处理SparkContext的runJob方法最终调用该方法
    *
    * @param jobId
    * @param finalRDD
    * @param func
    * @param partitions
    * @param callSite
    * @param listener
    * @param properties
    */
  private[scheduler] def handleJobSubmitted(jobId: Int,
                                            finalRDD: RDD[_],
                                            func: (TaskContext, Iterator[_]) => _,
                                            partitions: Array[Int],
                                            callSite: CallSite,
                                            listener: JobListener,
                                            properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite) //TODO 从后向前划分stage
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage) //TODO 提交Stage
  } //handleJobSubmitted 定义结束

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
                                                 dependency: ShuffleDependency[_, _, _],
                                                 callSite: CallSite,
                                                 listener: JobListener,
                                                 properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /**
    * Submits stage, but first recursively submits any missing parents.
    * <br>提交Satge，但是会递归提交父Stage<br>
    *
    * @param stage 实际上传入的是ResultStage
    */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        // 获取依赖的未提交的父stage
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          //TODO 如果父Stage都提交完成，则提交Stage
          submitMissingTasks(stage, jobId.get)
        } else {
          //TODO 如果有未提交的父Stage，则递归提交
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage //stage的其父stage还没有提交，故存储在waitingStages中
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /**
    * Called when stage's parents are available and we can now do its task.
    *
    * @param stage
    * @param jobId
    */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    //TODO 真正提交Stage的函数
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    /**
      * First figure out the indexes of partition ids to compute.
      * <br>存储需要计算的分区id<br>
      */
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    /**
      * Use the scheduling pool, job group, description, etc. from an ActiveJob associated with this Stage
      */
    val properties = jobIdToActiveJob(jobId).properties

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage => //在DAG Stage依赖关系中，除之后的Stage 外，全部为ShuffleMapStage
    outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    //根据partitionsToCompute获取其优先位置PreferredLocations，使计算离数据最近
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id)) }.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null //任务的二进制信息，并不是任务的二进制数据
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match { //TODO 序列化Task的Rdd和依赖信息
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }
      //TODO Stage信息是通过广播出去的
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    //TODO 根据ShuffleMapTask或ResultTask，用于后期创建TaskSet
    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id => //TODO 遍历存储需要计算的分区id，为每一个分区创建task ，最后返回一个task任务列表
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            //创建ShuffleMapTask
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,  //TODO 创建Task
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId)
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id => //遍历存储需要计算的分区id，为每一个分区创建task ，最后返回一个task任务列表
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            //创建ResultTask
            new ResultTask(stage.id, stage.latestInfo.attemptId,  //TODO 创建Task
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingPartitions ++= tasks.map(_.partitionId) //记录当前stage计算的分区
      logDebug("New pending partitions: " + stage.pendingPartitions)
      //TODO 重要！创建TaskSet并使用taskScheduler的submitTasks方法提交Stage
      taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      //提交完毕
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // thJobe stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage: ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)

      submitWaitingChildStages(stage)
    }
  } //submitMissingTasks End

  /**
    * Merge local values from a task into the corresponding accumulators previously registered
    * here on the driver.
    *
    * Although accumulators themselves are not thread-safe, this method is called only from one
    * thread, the one that runs the scheduling loop. This means we only handle one task
    * completion event at a time so we don't need to worry about locking the accumulators.
    * This still doesn't stop the caller from updating the accumulator outside the scheduler,
    * but that's not our problem since there's nothing we can do about that.
    */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.accumulables += acc.toInfo(Some(updates.value), Some(acc.value))
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  /**
    * Responds to a task finishing. This is called inside the event loop so it assumes that it can
    * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
    */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
      stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        stage.pendingPartitions -= task.partitionId
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                submitWaitingChildStages(shuffleStage)
              }
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingPartitions += task.partitionId

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId)) {
            abortStage(failedStage, s"$failedStage (${failedStage.name}) " +
              s"has failed the maximum allowable number of " +
              s"times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}. " +
              s"Most recent failure reason: ${failureMessage}", None)
          } else {
            if (failedStages.isEmpty) {
              // Don't schedule an event to resubmit failed stages if failed isn't empty, because
              // in that case the event will already have been scheduled.
              // TODO: Cancel running tasks in the stage
              logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
            failedStages += failedStage
            failedStages += mapStage
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
      // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event)

      case TaskResultLost =>
      // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | TaskKilled | UnknownReason =>
      // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
      // will abort the job.
    }
  }

  /**
    * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
    * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
    *
    * We will also assume that we've lost all shuffle blocks associated with the executor if the
    * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
    * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
    * presume all shuffle data related to this executor to be lost.
    *
    * Optionally the epoch during which the failure was caught can be passed to avoid allowing
    * stray fetch failures from possibly retriggering the detection of a node as lost.
    */
  private[scheduler] def handleExecutorLost(
                                             execId: String,
                                             filesLost: Boolean,
                                             maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
        "(epoch " + currentEpoch + ")")
    }
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
  }

  /**
    * Marks a stage as finished and removes it from the list of running stages.
    */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }

    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
    * Aborts all jobs depending on a particular Stage. This is called in response to a task set
    * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
    */
  private[scheduler] def abortStage(
                                     failedStage: Stage,
                                     reason: String,
                                     exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
                                           job: ActiveJob,
                                           failureReason: String,
                                           exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try {
              // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              } // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
    * Gets the locality information associated with a partition of a particular RDD.
    *<br>获取一个指定RDD的分区的locality information<br>
    * This method is thread-safe and is called from both DAGScheduler and SparkContext.
    *
    * @param rdd       whose partitions are to be looked at
    * @param partition to lookup locality information for
    * @return list of machines that are preferred by the partition
    */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
    * Recursive implementation for getPreferredLocs.
    *
    * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
    * methods (getCacheLocs()); please be careful when modifying this method, because any new
    * DAGScheduler state accessed by it may require additional synchronization.
    */
  private def getPreferredLocsInternal(
                                        rdd: RDD[_],
                                        partition: Int,
                                        visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

//TODO DAGScheduler 定义结束

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
    * The main event loop of the DAG scheduler.事件循环处理器，当消息来了之后发给doOnReceive
    *
    */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  /**
    *
    * @param event 事件消息是由org.apache.spark.scheduler.DAGSchedulerEventProcessLoop#onReceive发来的
    */
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      //TODO org.apache.spark.scheduler.DAGScheduler.handleJobSubmitted真正触发作业
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
