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

package org.apache.spark.streaming.kafka010

import java.{util => ju}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
  * A DStream where
  * each given Kafka topic/partition corresponds to an RDD partition.
  * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
  * of messages
  * per second that each '''partition''' will accept.
  *
  * @param locationStrategy    In most cases, pass in [[PreferConsistent]],
  *                            see [[LocationStrategy]] for more details.
  * @param executorKafkaParams Kafka
  *                            <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  *                            configuration parameters</a>.
  *                            Requires  "bootstrap.servers" to be set with Kafka broker(s),
  *                            NOT zookeeper servers, specified in host1:port1,host2:port2 form.
  * @param consumerStrategy    In most cases, pass in [[Subscribe]],
  *                            see [[ConsumerStrategy]] for more details
  * @tparam K type of Kafka message key
  * @tparam V type of Kafka message value
  */
private[spark] class DirectKafkaInputDStream[K, V](
                                                    _ssc: StreamingContext,
                                                    locationStrategy: LocationStrategy, //TODO 消费的位置策略信息
                                                    consumerStrategy: ConsumerStrategy[K, V], //TODO 包含topic信息和kafka配置信息
                                                    ppc: PerPartitionConfig //TODO 控制Kafka分区读取速率的参数
                                                  ) extends InputDStream[ConsumerRecord[K, V]](_ssc) with Logging with CanCommitOffsets {

  /**
    * 用户配置的kafka配置
    */
  val executorKafkaParams = {
    val ekp = new ju.HashMap[String, Object](consumerStrategy.executorKafkaParams)
    KafkaUtils.fixKafkaParams(ekp)
    ekp
  }

  /**
    * 当前的消费偏移信息，key为topic和partition的封装，value为offset值<br><br>
    * var currentOffsets = Map[TopicPartition, Long]()<br><br>
    * currentOffsets在DirectKafkaInputDStream#start()启动时候进行初始化，如果找不到offset的话就根据offset.reset策略进行处理
    */
  protected var currentOffsets = Map[TopicPartition, Long]()

  @transient private var kc: Consumer[K, V] = null

  def consumer(): Consumer[K, V] = this.synchronized {
    if (null == kc) {
      //
      kc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava)
    }
    kc
  }

  override def persist(newLevel: StorageLevel): DStream[ConsumerRecord[K, V]] = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  protected def getBrokers = {
    val c = consumer
    val result = new ju.HashMap[TopicPartition, String]()
    val hosts = new ju.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator() //获取到TopicPartition集合

    while (assignments.hasNext()) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator() //分区信息PartitionInfo
        while (infos.hasNext()) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    } //while end

    result
  }

  protected def getPreferredHosts: ju.Map[TopicPartition, String] = {
    locationStrategy match {
      case PreferBrokers => getBrokers
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
  }

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka 0.10 direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
    * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
    */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  /**
    *
    * @param offsets maxMessagesPerPartition方法实现了获取某个partition能消费到的message的数量
    * @return
    */
  protected[streaming] def maxMessagesPerPartition(offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {

    val estimatedRateLimit = rateController.map(_.getLatestRate())

    // calculate a per-partition rate limit based on current lag
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
          //TODO 通过PerPartitionConfig的maxRatePerPartition方法获取每一个分区消费消息速率
          val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)

          val backpressureRate = Math.round(lag / totalLag.toFloat * rate)

          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)
          } else backpressureRate)
        }
      //TODO 通过PerPartitionConfig的maxRatePerPartition方法获取每一个分区消费消息速率
      case None => offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp) }
    }

    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) => tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }

  /**
    * The concern here is that poll might consume messages despite being paused,
    * which would throw off consumer position.  Fix position if this happens.
    * <br><br>
    * 这个函数的实际功能就是寻找当前消费的位置offset，这个动作是对Kafka集群而言的
    */
  private def paranoidPoll(c: Consumer[K, V]): Unit = {
    val msgs = c.poll(0) //msgs迭代器
    if (!msgs.isEmpty) {
      //计算msgs中以“topic和partitions”分组，求每一组消息的offset
      //类型: msgs: ConsumerRecords[K, V]
      // position should be minimum offset per topicpartition
      msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) => //类型： acc: Map[TopicPartition, Long]，m: ConsumerRecord[K, V]
        val tp = new TopicPartition(m.topic, m.partition) //对消息集合msgs中的每条消息进行一个foldLeft操作
      //org.apache.kafka.clients.consumer.ConsumerRecord.offset是当前消息在分区中的offset
      //TODO 循环迭代获取TopicPartition的最小消息offset(最小消息offset其实就是消息的其实消费位置)
      val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
        //TODO 容易困惑的地方，acc是一个immutable的map，但是对其+操作将会返回一个新的map
        acc + (tp -> off) //计算下一消费的其实偏移位置
      } /*到此完成了寻找每一个TopicPartition的消息集合的最小offset定位*/ .foreach { case (tp, off) =>
        logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate") // （compensate ： 抵消;补偿，赔偿;报酬）
        //TODO fetch offsets that the consumer will use on the next poll(timeout).
        c.seek(tp, off) //移到下一次消费的其实偏移位置
      }
    }

  }

  /**
    * Returns the latest (highest) available offsets, taking new partitions into account.
    * <br><br>返回当前每一个分区的最新offset(最新的offset就是指最大的offset)
    */
  protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer
    //TODO 完成当前消费的起始offset定位
    paranoidPoll(c)

    //TODO Get the set of partitions currently assigned to this consumer.
    val parts = c.assignment().asScala // Set[TopicPartition]

    // make sure new partitions are reflected in currentOffsets
    //parts为最近的kafka集群的TopicPartition信息，currentOffsets为上一次消费的kafka集群的TopicPartition信息，所以相减为加入的分区
    val newPartitions = parts.diff(currentOffsets.keySet) //获取到所有分区，包含新加入的分区，x.diff(y)操作是在x集合中去掉和y共同的

    //TODO auto.offset.reset参数含义：当在kafka中没有初始的offset或者当前offset的消息不存在时候，如何进行处理。
    //TODO earliest：重新开始消费  ,latest 最新的位置消费 ，等等其他参数参考kafka文档(https://kafka.apache.org/0102/documentation.html#newconsumerconfigs)
    // position for new partitions determined by auto.offset.reset if no commit
    //TODO 所以此处对于新分区没有初始offset的话，会根据集群的auto.offset.reset配置进行相应处理
    //consumer的position方法是获取下一条即将被fetch的消息的offset
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap //该语句作用是将新分区添加到消费的分区集合中
    // don't want to consume messages, so pause
    c.pause(newPartitions.asJava)
    // find latest available offsets
    c.seekToEnd(currentOffsets.keySet.asJava) //移动每一个分区的offset到当前分区的end位置
    parts.map(tp => tp -> c.position(tp)).toMap
  }

  /**
    * limits the maximum number of messages per partition<br><br>
    * Clamp方法是根据Spark.streaming.kafka.maxRatePerPartition和backpressure这两个参数来
    * 设置当前block可以消费到的offset的（即untilOffset）
    *
    * @param offsets 每一个TopicPartition消费的start offset
    * @return 返回消费区区间的end offset
    */
  protected def clamp(offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    //clamp：在此处应该是锁定区间的意思
    //TODO maxMessagesPerPartition方法实现了获取某个partition能消费到的message的数量
    maxMessagesPerPartition(offsets).map { mmp => //mmp: Map[TopicPartition, Long]
      mmp.map { case (tp, messages) =>
        val uo = offsets(tp)
        //Math.min(当前消费的offset +要消费的消息量,partition最新的offset)
        tp -> Math.min(currentOffsets(tp) + messages, uo)
      }
    }.getOrElse(offsets)
  }

  /**
    *
    * Method that generates an RDD for the given time
    *
    * @param validTime 当前批次间隔的时间
    * @return Option[KafkaRDD[K, V]]
    */
  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {

    val untilOffsets = clamp(latestOffsets()) //TODO 重点业务，其中包含消息区间的确定和速率的控制


    // OffsetRange包含信息有：topic，partition，起始位置，结束位置
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      val fo = currentOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo) //生成每一个分区的消费区间
    }

    //TODO KafkaRDD构造函数的第三个参数比较重要：该参数定义了Kafka分区属于当前RDD数据的offset值
    val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, true)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    //TODO 汇报当前记录数目和元数据信息到InputInfoTracker
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset //TODO 过滤掉区间为空的offsetRange
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    //TODO InputInfoTracker是运行在Driver端,负责计算数据的监控
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    //将当前消费过的最新偏移设置到currentOffsets中
    currentOffsets = untilOffsets
    commitAll()
    Some(rdd)
  }

  /**
    * Method called to start receiving data.
    * <br><br>
    * 调用过程：org.apache.spark.streaming.scheduler.JobGenerator#startFirstTime()调用org.apache.spark.streaming.dstream.InputDStream#start()启动
    */

  override def start(): Unit = {
    val c = consumer
    paranoidPoll(c)
    //TODO 第一次启动，初始化currentOffsets
    if (currentOffsets.isEmpty) {
      currentOffsets = c.assignment().asScala.map { tp =>
        //TODO org.apache.kafka.clients.consumer.KafkaConsumer.position中的实现逻辑是当找不到offset时候会根据offset.reset策略进行初始化，
        //TODO 内部调用org.apache.kafka.clients.consumer.KafkaConsumer#updateFetchPositions实现
        tp -> c.position(tp) //Get the offset of the next record that will be fetched (if a record with that offset exists).
      }.toMap
    }

    // don't actually want to consume any messages, so pause all partitions
    c.pause(currentOffsets.keySet.asJava)
  }

  override def stop(): Unit = this.synchronized {
    if (kc != null) {
      kc.close()
    }
  }

  protected val commitQueue = new ConcurrentLinkedQueue[OffsetRange]
  protected val commitCallback = new AtomicReference[OffsetCommitCallback]

  /**
    * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
    *
    * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
    */
  def commitAsync(offsetRanges: Array[OffsetRange]): Unit = {
    commitAsync(offsetRanges, null)
  }

  /**
    * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
    *
    * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
    * @param callback     Only the most recently provided callback will be used at commit.
    */
  def commitAsync(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit = {
    commitCallback.set(callback)
    commitQueue.addAll(ju.Arrays.asList(offsetRanges: _*))
  }

  protected def commitAll(): Unit = {
    val m = new ju.HashMap[TopicPartition, OffsetAndMetadata]()
    var osr = commitQueue.poll()
    while (null != osr) {
      val tp = osr.topicPartition
      val x = m.get(tp)
      val offset = if (null == x) {
        osr.untilOffset
      } else {
        Math.max(x.offset, osr.untilOffset)
      }
      m.put(tp, new OffsetAndMetadata(offset))
      osr = commitQueue.poll()
    }
    if (!m.isEmpty) {
      consumer.commitAsync(m, commitCallback.get)
    }
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new KafkaRDD[K, V](
          context.sparkContext,
          executorKafkaParams,
          b.map(OffsetRange(_)),
          getPreferredHosts,
          // during restore, it's possible same partition will be consumed from multiple
          // threads, so dont use cache
          false
        )
      }
    }
  }

  /**
    * A RateController to retrieve the rate from RateEstimator.
    */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }

}
