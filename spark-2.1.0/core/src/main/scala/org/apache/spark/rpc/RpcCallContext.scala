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

/**
  * A callback that [[RpcEndpoint]] can use to send back a message or failure. It's thread-safe
  * and can be called in any thread.<br><br>
  * RpcEndpoint的回调用来反馈消息或者失败，它是线程安全的。
  *
  */
private[spark] trait RpcCallContext {

  /**
    * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]] will be called.<br><br>
    *
    */
  def reply(response: Any): Unit

  /**
    * Report a failure to the sender.
    * 发送消息失败的原因
    */
  def sendFailure(e: Throwable): Unit

  /**
    * The sender of this message.
    * <br><br>消息发送者的地址
    */
  def senderAddress: RpcAddress
}
