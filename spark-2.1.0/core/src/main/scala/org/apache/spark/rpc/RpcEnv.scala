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

package org.apache.spark.rpc

import java.io.File
import java.nio.channels.ReadableByteChannel

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils


/**
  * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
  * so that it can be created via Reflection.<br>
  * RpcEnv的实现必须有一个带有空方法的RpcEnvFactory，它需要被反射创建
  */
private[spark] object RpcEnv {

  /**
    * 在Master、Worker启动时候会调用这个函数
    *
    * @param name
    * @param host
    * @param port
    * @param conf
    * @param securityManager
    * @param clientMode
    * @return new NettyRpcEnvFactory().create(config)
    */
  def create(
              name: String,
              host: String,
              port: Int,
              conf: SparkConf,
              securityManager: SecurityManager,
              clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, clientMode)
  }

  def create(
              name: String,
              bindAddress: String,
              advertiseAddress: String,
              port: Int,
              conf: SparkConf,
              securityManager: SecurityManager,
              clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}


/**
  * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
  * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
  * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
  * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
  * sender, or logging them if no such sender or `NotSerializableException`.
  *
  * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.<br><br>
  *
  * RpcEnv是一个RPC环境，需要将RpcEndpoint注册到RpcEnv上，目的是接受消息。RpcEnv负责处理RpcEndpointRef的消息或者是远端节点的消息，
  * 转发消息到对应的RpcEndpoint上，对于没有捕获的异常使用RpcCallContext.sendFailure发送回给发送者，如果没有该sender的话会日志记录
  *
  *<br>-------------------------------------------------------------------------------------------------------------------<br>
  * 一个RpcEnv是一个RPC环境对象，它负责管理RpcEndpoint的注册，以及如何从一个RpcEndpoint获取到一个RpcEndpointRef。
  * RpcEndpoint是一个通信端，例如Spark集群中的Master，或Worker，都是一个RpcEndpoint。但是，如果想要与一个RpcEndpoint端进行通信，
  * 一定需要获取到该RpcEndpoint一个RpcEndpointRef，而获取该RpcEndpointRef只能通过一个RpcEnv环境对象来获取。所以说，
  * 一个RpcEnv对象才是RPC通信过程中的“指挥官”，在RpcEnv类中，有一个核心的方法：
  * def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
  * 通过上面方法，可以注册一个RpcEndpoint到RpcEnv环境对象中，有RpcEnv来管理RpcEndpoint到RpcEndpointRef的绑定关系。
  * 在注册RpcEndpoint时，每个RpcEndpoint都需要有一个唯一的名称。
  *
  * @param conf
  */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  def address: RpcAddress

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
    */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
    * This is a blocking action.
    */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  def awaitTermination(): Unit

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  def deserialize[T](deserializationAction: () => T): T

  /**
    * Return the instance of the file server used to serve files. This may be `null` if the
    * RpcEnv is not operating in server mode.
    */
  def fileServer: RpcEnvFileServer

  /**
    * Open a channel to download a file from the given URI. If the URIs returned by the
    * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
    * retrieve the files.
    *
    * @param uri URI with location of the file.
    */
  def openChannel(uri: String): ReadableByteChannel
}

/**
  * A server used by the RpcEnv to server files to other processes owned by the application.
  *
  * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
  * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
  */
private[spark] trait RpcEnvFileServer {

  /**
    * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
    * to executors when they're stored on the driver's local file system.
    *
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    */
  def addFile(file: File): String

  /**
    * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
    * `SparkContext.addJar`.
    *
    * @param file Local file to serve.
    * @return A URI for the location of the file.
    */
  def addJar(file: File): String

  /**
    * Adds a local directory to be served via this file server.
    *
    * @param baseUri Leading URI path (files can be retrieved by appending their relative
    *                path to this base URI). This cannot be "files" nor "jars".
    * @param path    Path to the local directory.
    * @return URI for the root of the directory in the file server.
    */
  def addDirectory(baseUri: String, path: File): String

  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

private[spark] case class RpcEnvConfig(
                                        conf: SparkConf,
                                        name: String,
                                        bindAddress: String,
                                        advertiseAddress: String,
                                        port: Int,
                                        securityManager: SecurityManager,
                                        clientMode: Boolean)
