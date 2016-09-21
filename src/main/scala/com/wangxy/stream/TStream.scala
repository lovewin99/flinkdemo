package com.wangxy.stream

import org.apache.flink.streaming.api.scala._

/**
 *
 * Created by wangxy on 16-9-19.
 */
object TStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
//    val text = env.readTextFile("/home/wangxy/data/tstream/hello.txt")
    val text = env.socketTextStream("localhost", 9999)

    /***************************** 时间窗口 ****************************************/
//    val counts = text.flatMap{_.split(" ")}.map{(_, 1)}.keyBy(0).timeWindow(Time.seconds(5)).sum(1)

    /***************************** 数量窗口 ****************************************/
//    val counts = text.flatMap{_.split(" ")}.map{(_, 1)}.keyBy(0).countWindow(5).sum(1)

    /***************************** 直接落地 ****************************************/
//    val counts = text.flatMap{_.split(" ")}.map{(_, 1)}

    /***************************** 迭代     ****************************************/
//    val counts = text.flatMap{_.split(" ")}.iterate{iteration =>
//        val minusOne = iteration.map( v => v.toInt - 1)
//        val stillGreaterThanZero = minusOne.filter (_ > 0).map{_.toString}
//        val lessThanZero = minusOne.filter(_ <= 0)
//        (stillGreaterThanZero, lessThanZero)
//    }


//    counts.print()
    env.execute("Window Stream WordCount")


  }

}
