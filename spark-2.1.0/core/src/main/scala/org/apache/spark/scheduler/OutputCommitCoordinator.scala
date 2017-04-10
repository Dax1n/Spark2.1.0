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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage

private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)

/**
  * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
  * policy.
  *
  * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
  * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
  * commit output will be forwarded to the driver's OutputCommitCoordinator.
  *
  * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
  * for an extensive design discussion.
  * <br><br>
  * 确定任务是否可以把输出提到到HFDS的管理者。 使用先提交者胜的策略。
  * 在driver 端和执行器端都要初始化OutputCommitCoordinator。在执行器端，有一个指向driver
  * 端OutputCommitCoordinatorEndpoing对象的引用，所以提交输出的请求到被转发到driver端的OutputCommitCoordinator.
  * 这个类在Spark-4879提出，如果想要更多的设计讨论，请查阅JIRA。
  * 这个类主要使用一个authorizedCommittersByStage对象，这个对象有所有stage的各个partition的状态，
  * 刚开始，在stageStart时，此stage的各partitions的状态是NO_AUTHORIZED_COMMITER。当有任务完成时，
  * 会调用canCommit方法来判断是否可以提交，这个请求会在driver端调用handleAskPermissionToCommit，
  * 在此方法里，如果判断相应partition的状态是NO_AUTHORIZED_COMMITER，则会返回true，否则返回false。
  * 如果提交的任务完成后，调度器会调用taskCompleted方法，如果成功，则不处理，如果任务失败，
  * 则判断这个任务是否是相应partition的提交task，如果是，代表提交失败，则把相应partition设置为NO_AUTHORIZED_COMMITER，
  * 这样这个partition的其它task还可以处理提交。
  *
  *
  */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int

  private val NO_AUTHORIZED_COMMITTER: TaskAttemptNumber = -1

  /**
    * Map from active stages's id => partition id => task attempt with exclusive lock on committing
    * output for that partition.
    *
    * Entries are added to the top-level map when stages start and are removed they finish
    * (either successfully or unsuccessfully).
    *
    * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
    */
  private val authorizedCommittersByStage = mutable.Map[StageId, Array[TaskAttemptNumber]]()

  /**
    * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
    */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
  }

  /**
    * Called by tasks to ask whether they can commit their output to HDFS.
    *
    * If a task attempt has been authorized to commit, then all other attempts to commit the same
    * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
    * lost), then a subsequent task attempt may be authorized to commit its output.
    *
    * @param stage         the stage number
    * @param partition     the partition number
    * @param attemptNumber how many times this task has been attempted
    *                      (see [[TaskContext.attemptNumber()]])
    * @return true if this task is authorized to commit, false otherwise
    */
  def canCommit(
                 stage: StageId,
                 partition: PartitionId,
                 attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
    * Called by the DAGScheduler when a stage starts.
    *
    * @param stage          the stage id.
    * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
    *                       the maximum possible value of `context.partitionId`).
    */
  private[scheduler] def stageStart(
                                     stage: StageId,
                                     maxPartitionId: Int): Unit = {
    val arr = new Array[TaskAttemptNumber](maxPartitionId + 1)
    java.util.Arrays.fill(arr, NO_AUTHORIZED_COMMITTER)
    synchronized {
      authorizedCommittersByStage(stage) = arr
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
                                        stage: StageId,
                                        partition: PartitionId,
                                        attemptNumber: TaskAttemptNumber,
                                        reason: TaskEndReason): Unit = synchronized {
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case denied: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
          s"attempt: $attemptNumber")
      case otherReason =>
        if (authorizedCommitters(partition) == attemptNumber) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          authorizedCommitters(partition) = NO_AUTHORIZED_COMMITTER
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      authorizedCommittersByStage.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
                                                      stage: StageId,
                                                      partition: PartitionId,
                                                      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters(partition) match {
          case NO_AUTHORIZED_COMMITTER =>
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            authorizedCommitters(partition) = attemptNumber
            true
          case existingCommitter =>
            logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; existingCommitter = $existingCommitter")
            false
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing attempt number $attemptNumber of" +
          s"partition $partition to commit")
        false
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
                                                        override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init")

    // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
    }
  }

}
