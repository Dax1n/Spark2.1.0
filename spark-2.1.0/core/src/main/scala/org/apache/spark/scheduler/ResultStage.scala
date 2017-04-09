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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
  *
  *
  * * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
  * The ResultStage object captures the function to execute, `func`, which will be applied to each
  * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
  * of the RDD, for actions like first() and lookup().
  * <br><br>ResultStages会讲一个函数在RDD的一些分区上执行并结算该action的结果，ResultStage会触发函数的执行
  * ，函数将会应用在每一个分区上，但是有些ResultStages可能只会运行在RDD的部分分区上，例如first() 和lookup()算子
  * <br>
  *
  * @param id         Unique stage ID
  * @param rdd        RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
  *                   on, while for a result stage, it's the target RDD that we ran an action on
  * @param func
  * @param partitions
  * @param parents    List of stages that this stage depends on (through shuffle dependencies).
  * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
  * @param callSite   Location in the user program associated with this stage: either where the target
  *                   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
  */
private[spark] class ResultStage(
                                  id: Int,
                                  rdd: RDD[_],
                                  val func: (TaskContext, Iterator[_]) => _,
                                  val partitions: Array[Int],
                                  parents: List[Stage],
                                  firstJobId: Int,
                                  callSite: CallSite)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite) {

  /**
    * The active job for this result stage. Will be empty if the job has already finished
    * (e.g., because the job was cancelled).
    */
  private[this] var _activeJob: Option[ActiveJob] = None

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
    * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
    *
    * This can only be called when there is an active job.
    */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}
