package com.wangxy.stream

import java.util.Properties

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 *
 * Created by wangxy on 16-9-19.
 */
object FromKafka {


  private val ZOOKEEPER_HOST = "cloud136:2181,cloud136:2181,cloud136:2181"
  private val KAFKA_BROKER = "cloud136:9092,cloud138:9092,cloud139:9092"
  private val TRANSACTION_GROUP = "a"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //topicd的名字是mon1，schema默认使用SimpleStringSchema()即可
    val transaction = env.addSource(new FlinkKafkaConsumer08[String]("mon1", new SimpleStringSchema(), kafkaProps))

//    val res = transaction.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    val res = transaction

    res.print()

    env.execute()

  }

}
