package com.wangxy.stream

//import java.net.{InetAddress, InetSocketAddress}
//import java.util
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//import org.elasticsearch.action.index.{IndexRequest, IndexRequestBuilder}
//import org.elasticsearch.client.Requests
//import org.elasticsearch.common.transport.InetSocketTransportAddress
//import scala.collection.mutable.{ListBuffer, Map}
//import scala.collection.JavaConversions._
//
//import java.util.Properties
//
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala._

//import com.tescomm.TestElasticsearchSinkFunction

/**
 *
 * Created by wangxy on 16-9-20.
 */
object Kafka2Es {

//  private val ZOOKEEPER_HOST = "cloud136:2181,cloud136:2181,cloud136:2181"
//  private val KAFKA_BROKER = "cloud136:9092,cloud138:9092,cloud139:9092"
//  private val TRANSACTION_GROUP = "a"
//
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.enableCheckpointing(1000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//
//    // configure Kafka consumer
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
//    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
//    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
//
//
//    val transaction = env.addSource(new FlinkKafkaConsumer08[String]("mon1", new SimpleStringSchema(), kafkaProps))
//
//    val config = Map[String, String]()
//    config.put("bulk.flush.max.actions", "1")
//    config.put("cluster.name", "my-cluster-name")
//
//    val transports = new util.ArrayList[InetSocketAddress]()
//
//    transports += new InetSocketAddress(InetAddress.getByName("localhost"), 9300)
//
////    val fc = new ElasticsearchSink(config, transports, new IndexRequestBuilder[String] {
////      override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
////        val json = new util.HashMap[String, AnyRef]
////        json.put("data", element)
////        println("SENDING: " + element)
////        Requests.indexRequest.index("my-index").`type`("my-type").source(json)
////      }
////    })
//
////    val fc = new ElasticsearchSink(config, new IndexRequestBuilder[String] {
////      override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
////        val json = new util.HashMap[String, AnyRef]
////        json.put("data", element)
////        println("SENDING: " + element)
////        Requests.indexRequest.index("my-index").`type`("my-type").source(json)
////      }
////    })
////
////    transaction.addSink()
//
//    env.execute()
//
//  }

}
