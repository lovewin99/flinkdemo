package com.wangxy.stream

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch2.{ElasticsearchSink, ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
//import scala.collection.JavaConversions.mapAsJavaMap
//import scala.collection.JavaConversions.seqAsJavaList

import java.util.Properties

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

//import com.wangxy.stream.TestElasticsearchSinkFunction

/**
 *
 * Created by wangxy on 16-9-19.
 */
object ToEs {

  private val ZOOKEEPER_HOST = "cloud136:2181,cloud136:2181,cloud136:2181"
  private val KAFKA_BROKER = "cloud136:9092,cloud138:9092,cloud139:9092"
  private val TRANSACTION_GROUP = "a"

  class TestElasticsearchSinkFunction extends ElasticsearchSinkFunction[String]{

    def createIndexRequest(element: String): IndexRequest = {
      val f = element+"hello"
      val l = element+"world"
      val m = Map("first_name"->f, "last_name"->l, "age"->element)
      Requests.indexRequest().index("test11").source(m)
    }


    override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
//       indexer.add(createIndexRequest(element))
      println(element)
    }

  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    val config = Map.empty[String, String]
    config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1")
    config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "1")

//    val transports = Seq[InetSocketAddress]()
//    transports += new InetSocketAddress(InetAddress.getByName("localhost"), 9300)

    val transports = Seq(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))

    //topicd的名字是mon1，schema默认使用SimpleStringSchema()即可
    val transaction = env.addSource(new FlinkKafkaConsumer08[String]("mon1", new SimpleStringSchema(), kafkaProps)).flatMap(_.split(" "))

    //    val res = transaction.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

//    transaction.print()

    transaction.addSink(new ElasticsearchSink[String](config, transports, new TestElasticsearchSinkFunction))

    env.execute()

  }

}
