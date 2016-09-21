package com.wangxy.bat

//import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.common.functions.{RichMapFunction, GroupReduceFunction}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import scala.collection.JavaConversions._
import org.apache.flink.configuration.Configuration

/**
 *
 * Created by wangxy on 16-9-18.
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    // execution 模式
//    val env = ExecutionEnvironment.getExecutionEnvironment

    // local 模式
    val env = ExecutionEnvironment.createLocalEnvironment()
    val ds = env.readTextFile("/home/wangxy/data/hello.txt")

    /***************************** test1 ****************************************/
//    val res = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
//
//    res.map{x =>
//      println(s"x = $x")
//      x
//    }.print()

    /***************************** test reduceGroup ****************************************/
//    val res = ds.flatMap(_.split(" ")).map((_, 1)).reduceGroup(x => x.foreach(println)).print()

    /***************************** test broadcast perameter****************************************/
//    val d1 = env.fromElements("a")
//    val t1 = new Configuration()
//    t1.setInteger("t1", 1)

//     method 1
//    val res = ds.flatMap(_.split(" ")).map{new RichMapFunction[String, String]() {
//      //var broadcastSet = List.empty[String]
//      var broadcastSet: Traversable[String] = null
//
//      override def open(config: Configuration): Unit = {
//        // 3. Access the broadcasted DataSet as a Collection
//        broadcastSet = getRuntimeContext.getBroadcastVariable[String]("d1")
//      }
//
//      def map(in: String): String = {
//        println(s"broadcastSet = $broadcastSet")
//        ""
//      }
//    }
//    }.withBroadcastSet(d1, "d1").print()

//     method 2
//    val res = ds.flatMap(_.split(" ")).map(new MapWithBroadCast("d1")).withBroadcastSet(d1, "d1").withParameters(t1).print()

    /***************************** test bulk iterations ****************************************/

    // Create initial DataSet
//    val initial = env.fromElements(0)
//
//    val count = initial.iterate(10) { iterationInput: DataSet[Int] =>
//      val result = iterationInput.map { i =>
//        val x = Math.random()
//        val y = Math.random()
//        val r = i + (if (x * x + y * y < 1) 1 else 0)
//        println(s"i = $i   r = $r")
//        r
//      }
////      val s = result.map{x => println(x); 1}
////      s
//      result
//    }
//
//    count.print()

//    val result = count map { c => println(s"c = $c"); c / 10000.0 * 4 }
//
//    result.print()

//    val a = List(1,2,3,4)
//    val b = a.map{x=> println(x); x+1}
//    val c = b.map{x => println(x); x+1}
//    c.foreach(println)

    /***************************** test delta iterations ****************************************/

  }

}
