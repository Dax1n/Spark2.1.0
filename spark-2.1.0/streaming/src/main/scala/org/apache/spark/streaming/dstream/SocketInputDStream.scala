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

package org.apache.spark.streaming.dstream

import java.io._
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.NextIterator

/**
  *
  * @param _ssc Streaming context that will execute this input stream
  * @param host
  * @param port
  * @param bytesToObjects
  * @param storageLevel
  * @param ev$1
  * @tparam T Class type of the object of this stream
  */
private[streaming]
class SocketInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](_ssc) {

  /**
    *
    * @return  new SocketReceiver(host, port, bytesToObjects, storageLevel)
    */
  def getReceiver(): Receiver[T] = {
    new SocketReceiver(host, port, bytesToObjects, storageLevel)
  }
}

/**
  *
  * @param host
  * @param port
  * @param bytesToObjects
  * @param storageLevel
  * @param ev$1
  * @tparam T
  */
private[streaming]
class SocketReceiver[T: ClassTag](
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends Receiver[T](storageLevel) with Logging {

  private var socket: Socket = _

  def onStart() {

    logInfo(s"Connecting to $host:$port")
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
    logInfo(s"Connected to $host:$port")

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // in case restart thread close it twice
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
        logInfo(s"Closed socket to $host:$port")
      }
    }
  }

  /** Create a socket connection and receive data until receiver is stopped */
  def receive() {
    try {
      //TODO bytesToObjects函数中，当没有数据可读时候发生阻塞
      val iterator = bytesToObjects(socket.getInputStream())

      while(!isStopped && iterator.hasNext) {
        //TODO store方法是来至于：org.apache.spark.streaming.receiver.Receiver.store(T)
        store(iterator.next())
      }
      if (!isStopped()) {
        restart("Socket data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      onStop()
    }
  }
}

private[streaming]
object SocketReceiver  {

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = new BufferedReader(
      new InputStreamReader(inputStream, StandardCharsets.UTF_8))

    new NextIterator[String] {
      /**
        * 当没有数据可获得时候发生阻塞
        * @return U, or set 'finished' when done
        */
      protected override def getNext() = {
        //TODO 当nc控制台没有数据输入时候此处会阻塞
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }
}
