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

package org.apache.spark.streaming.util

import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock}

/**
  * 底层通过线程实现的一个定时器
  *
  * @param clock    Spark时钟
  * @param period   定时器间隔
  * @param callback 定时执行的Task
  * @param name     底层实现定时器的线程的名字
  */
private[streaming] class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  extends Logging {

  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)

    override def run() {
      loop
    } //loop是一个死循环，定时触发执行
  }

  @volatile private var prevTime = -1L
  @volatile private var nextTime = -1L
  @volatile private var stopped = false

  /**
    * Get the time when this timer will fire if it is started right now.<br>获取RecurringTimer的启动时间，然后在该事件进行启动<br>
    * The time will be a multiple of this timer's period and more than
    * current system time.（<br><br>时间将是该计时器周期的倍数，并且大于当前系统时间。）
    * <br><br><br>
    * 例如：当前时间 currentTime = 1502256785725，则调用getStartTime() 返回值则为：1502256786000
    * <br>所有就会在1502256785725的整数倍间隔的未来时间1502256786000启动RecurringTimer
    *
    */
  def getStartTime(): Long = {
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
    * Get the time when the timer will fire if it is restarted right now.
    * This time depends on when the timer was started the first time, and was stopped
    * for whatever reason. The time must be a multiple of this timer's period and
    * more than current time.
    */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
    * Start at the given start time.
    * @param startTime 启动的时间
    * @return 返回启动的时间
    */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
    * Start at the earliest time it can start based on the period.
    */
  def start(): Long = {
    start(getStartTime())
  }

  /**
    * Stop the timer, and return the last time the callback was made.
    *
    * @param interruptTimer True will interrupt the callback if it is in progress (not guaranteed to
    *                       give correct time in this case). False guarantees that there will be at
    *                       least one callback after `stop` has been called.
    */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logInfo("Stopped timer for " + name + " after time " + prevTime)
    }
    prevTime
  }

  /**
    * 定间隔时间执行RecurringTimer的任务
    */
  private def triggerActionForNextInterval(): Unit = {
    //TODO 第一次执行时候nextTime在start方法中被设置为启动时间（启动时间是在调用启动时刻的一个将来整数据倍间隔的将来时间）
    clock.waitTillTime(nextTime)//一直阻塞到当前时间
    callback(nextTime)
    prevTime = nextTime
    nextTime += period
    logDebug("Callback for " + name + " called at time " + prevTime)
  }

  /**
    * Repeatedly call the callback every interval.
    * <br>一个标记为控制的死循环
    */
  private def loop() {
    try {
      while (!stopped) {
        triggerActionForNextInterval()
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer extends Logging {

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}

