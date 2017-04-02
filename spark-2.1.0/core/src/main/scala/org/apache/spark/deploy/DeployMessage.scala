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

package org.apache.spark.deploy

import scala.collection.immutable.List

import org.apache.spark.deploy.ExecutorState.ExecutorState
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.RecoveryState.MasterState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
  * 分布式消息的父类
  */
private[deploy] sealed trait DeployMessage extends Serializable

/**
  * Contains messages sent between Scheduler endpoint nodes.
  */
private[deploy] object DeployMessages {

  // Worker to Master

  /**
    *
    * Worker向Master发送的消息
    *
    * @param id
    * @param host
    * @param port
    * @param worker
    * @param cores
    * @param memory
    * @param workerWebUiUrl
    */
  case class RegisterWorker(
                             id: String,
                             host: String,
                             port: Int,
                             worker: RpcEndpointRef,
                             cores: Int,
                             memory: Int,
                             workerWebUiUrl: String)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert(port > 0)
  }

  /**
    *
    * @param appId
    * @param execId
    * @param state
    * @param message
    * @param exitStatus
    */
  case class ExecutorStateChanged(
                                   appId: String,
                                   execId: Int,
                                   state: ExecutorState,
                                   message: Option[String],
                                   exitStatus: Option[Int])
    extends DeployMessage


  /**
    *
    * @param driverId
    * @param state
    * @param exception
    */
  case class DriverStateChanged(
                                 driverId: String,
                                 state: DriverState,
                                 exception: Option[Exception])
    extends DeployMessage

  /**
    *
    * @param id
    * @param executors
    * @param driverIds
    */
  case class WorkerSchedulerStateResponse(id: String, executors: List[ExecutorDescription],
                                          driverIds: Seq[String])

  /**
    * A worker will send this message to the master when it registers with the master. Then the
    * master will compare them with the executors and drivers in the master and tell the worker to
    * kill the unknown executors and drivers.
    */
  case class WorkerLatestState(
                                id: String,
                                executors: Seq[ExecutorDescription],
                                driverIds: Seq[String]) extends DeployMessage

  /**
    * 心跳信息
    *
    * @param workerId
    * @param worker
    */
  case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage

  // Master to Worker
  /**
    * Master向Worker回复的消息
    */
  sealed trait RegisterWorkerResponse

  /**
    *
    * @param master
    * @param masterWebUiUrl
    */
  case class RegisteredWorker(master: RpcEndpointRef, masterWebUiUrl: String) extends DeployMessage
    with RegisterWorkerResponse

  /**
    *
    * @param message
    */
  case class RegisterWorkerFailed(message: String) extends DeployMessage with RegisterWorkerResponse


  case object MasterInStandby extends DeployMessage with RegisterWorkerResponse

  /**
    *
    * @param masterUrl
    */
  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  /**
    *
    * @param masterUrl
    * @param appId
    * @param execId
    */
  case class KillExecutor(masterUrl: String, appId: String, execId: Int) extends DeployMessage

  /**
    *
    * @param masterUrl
    * @param appId
    * @param execId
    * @param appDesc
    * @param cores
    * @param memory
    */
  case class LaunchExecutor(
                             masterUrl: String,
                             appId: String,
                             execId: Int,
                             appDesc: ApplicationDescription,
                             cores: Int,
                             memory: Int)
    extends DeployMessage

  /**
    *
    * @param driverId
    * @param driverDesc
    */
  case class LaunchDriver(driverId: String, driverDesc: DriverDescription) extends DeployMessage

  /**
    *
    * @param driverId
    */
  case class KillDriver(driverId: String) extends DeployMessage

  /**
    *
    * @param id
    */
  case class ApplicationFinished(id: String)

  // Worker internal

  /**
    * Sent to Worker endpoint periodically for cleaning up app folders
    */
  case object WorkDirCleanup

  /**
    * used when a worker attempts to reconnect to a master
    */
  case object ReregisterWithMaster

  // used when a worker attempts to reconnect to a master

  // AppClient to Master


  /**
    *
    * @param appDescription
    * @param driver
    */
  case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef) extends DeployMessage

  /**
    *
    * @param appId
    */
  case class UnregisterApplication(appId: String)

  /**
    *
    * @param appId
    */
  case class MasterChangeAcknowledged(appId: String)

  /**
    *
    * @param appId
    * @param requestedTotal
    */
  case class RequestExecutors(appId: String, requestedTotal: Int)

  /**
    *
    * @param appId
    * @param executorIds
    */
  case class KillExecutors(appId: String, executorIds: Seq[String])

  /**
    *
    * @param appId
    * @param master
    */
  // Master to AppClient
  /**
    *
    * @param appId
    * @param master
    */
  case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends DeployMessage


  /**
    *
    * @param id
    * @param workerId
    * @param hostPort
    * @param cores
    * @param memory
    */
  case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
    // TODO(matei): replace hostPort with host
    Utils.checkHostPort(hostPort, "Required hostport")
  }

  /**
    *
    * @param id
    * @param state
    * @param message
    * @param exitStatus
    * @param workerLost
    */
  case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
                             exitStatus: Option[Int], workerLost: Boolean)

  /**
    *
    * @param message
    */
  case class ApplicationRemoved(message: String)

  // DriverClient <-> Master
  /**
    *
    * @param driverDescription
    */
  case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage

  /**
    *
    * @param master
    * @param success
    * @param driverId
    * @param message
    */
  case class SubmitDriverResponse(
                                   master: RpcEndpointRef, success: Boolean, driverId: Option[String], message: String)
    extends DeployMessage

  /**
    *
    * @param driverId
    */
  case class RequestKillDriver(driverId: String) extends DeployMessage

  /**
    *
    * @param master
    * @param driverId
    * @param success
    * @param message
    */
  case class KillDriverResponse(
                                 master: RpcEndpointRef, driverId: String, success: Boolean, message: String)
    extends DeployMessage

  /**
    *
    * @param driverId
    */
  case class RequestDriverStatus(driverId: String) extends DeployMessage

  /**
    *
    * @param found
    * @param state
    * @param workerId
    * @param workerHostPort
    * @param exception
    */
  case class DriverStatusResponse(found: Boolean, state: Option[DriverState],
                                  workerId: Option[String], workerHostPort: Option[String], exception: Option[Exception])

  // Internal message in AppClient

  case object StopAppClient

  // Master to Worker & AppClient
  /**
    *
    * @param master
    * @param masterWebUiUrl
    */
  case class MasterChanged(master: RpcEndpointRef, masterWebUiUrl: String)

  // MasterWebUI To Master
  /**
    *
    */
  case object RequestMasterState

  // Master to MasterWebUI
  /**
    *
    * @param host
    * @param port
    * @param restPort
    * @param workers
    * @param activeApps
    * @param completedApps
    * @param activeDrivers
    * @param completedDrivers
    * @param status
    */
  case class MasterStateResponse(
                                  host: String,
                                  port: Int,
                                  restPort: Option[Int],
                                  workers: Array[WorkerInfo],
                                  activeApps: Array[ApplicationInfo],
                                  completedApps: Array[ApplicationInfo],
                                  activeDrivers: Array[DriverInfo],
                                  completedDrivers: Array[DriverInfo],
                                  status: MasterState) {

    Utils.checkHost(host, "Required hostname")
    assert(port > 0)

    def uri: String = "spark://" + host + ":" + port

    def restUri: Option[String] = restPort.map { p => "spark://" + host + ":" + p }
  }

  //  WorkerWebUI to Worker
  /**
    *
    */
  case object RequestWorkerState

  // Worker to WorkerWebUI
  /**
    *
    * @param host
    * @param port
    * @param workerId
    * @param executors
    * @param finishedExecutors
    * @param drivers
    * @param finishedDrivers
    * @param masterUrl
    * @param cores
    * @param memory
    * @param coresUsed
    * @param memoryUsed
    * @param masterWebUiUrl
    */
  case class WorkerStateResponse(host: String, port: Int, workerId: String,
                                 executors: List[ExecutorRunner], finishedExecutors: List[ExecutorRunner],
                                 drivers: List[DriverRunner], finishedDrivers: List[DriverRunner], masterUrl: String,
                                 cores: Int, memory: Int, coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String) {

    Utils.checkHost(host, "Required hostname")
    assert(port > 0)
  }

  /**
    * Liveness checks in various places
    */
  case object SendHeartbeat

}
