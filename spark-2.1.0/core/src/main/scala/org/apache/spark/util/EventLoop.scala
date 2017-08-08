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

package org.apache.spark.util

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
  * An event loop to receive events from the caller and process all events in the event thread. It
  * will start an exclusive event thread to process all events.
  *
  * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
  * handle events in time to avoid the potential OOM.
  * <br><br>
  * 内部一个线程负责处理消息
  *
  */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  /**eventQueue是一个事件队列*/
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()
  /**事件循环处理器的启动或结束标记*/
  private val stopped = new AtomicBoolean(false)

  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {
            //TODO 一旦有事件，则调用onReceive进行消息处理
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
    * Put the event into the event queue. The event thread will process it later.
    */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  /**
    * Return if the event thread has already been started but not yet stopped.
    */
  def isActive: Boolean = eventThread.isAlive

  /**
    * Invoked when `start()` is called but before the event thread starts.
    * 生命周期方法
    */
  protected def onStart(): Unit = {}

  /**
    * Invoked when `stop()` is called and the event thread exits.
    * 生命周期方法
    */
  protected def onStop(): Unit = {}

  /**
    * Invoked in the event thread when polling events from the event queue.
    *
    * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
    * and cannot process events in time. If you want to call some blocking actions, run them in
    * another thread.
    * <br> 由于不同业务的处理方式不同，所以定义为抽象方法，有具体子类根据具体业务实现<br><br>
    * 生命周期方法
    */
  protected def onReceive(event: E): Unit

  /**
    * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
    * will be ignored.
    *
    *  <br> 由于不同业务的处理方式不同，所以定义为抽象方法，有具体子类根据具体业务实现
    */
  protected def onError(e: Throwable): Unit

}
