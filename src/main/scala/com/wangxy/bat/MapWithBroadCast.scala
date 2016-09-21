package com.wangxy.bat

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConversions._

/**
 *
 * Created by wangxy on 16-9-19.
 */
class MapWithBroadCast(in: String) extends RichMapFunction[String, String]{
  var broadcastSet: Traversable[String] = null
  var parameter1: Int = 1

  @ Override
  override def open(config: Configuration): Unit = {
    // 3. Access the broadcasted DataSet as a Collection
    broadcastSet = getRuntimeContext.getBroadcastVariable[String](in)
    parameter1 = config.getInteger("t1", 100)

  }

  @ Override
  def map(in: String): String = {
    println(s"broadcastSet = $broadcastSet")
    println(s"parameter1 = $parameter1")
    ""
  }
}
