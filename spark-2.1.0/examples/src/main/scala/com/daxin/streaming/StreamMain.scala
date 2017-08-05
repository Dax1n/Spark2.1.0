package com.daxin.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Daxin on 2017/8/4.
  */
object StreamMain {
  def main(args: Array[String]) {

    val conf =new SparkConf()

    conf.setAppName("socketStream")
    conf.setMaster("local[*]")
    val sc =new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc,Seconds(2))

    //TODO 最终创建一个SocketInputDStream返回
    val line = ssc.socketTextStream("node",9999) //基于Reciver模式，所以线程数目需要大于1，否则只能接受数据无法处理数据

    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //TODO 核心代码入口点，在org.apache.spark.streaming.StreamingContext.start方法中启动JobScheduler，开启接受数据并进行计算
    ssc.start()
    ssc.awaitTermination()

  }

}
