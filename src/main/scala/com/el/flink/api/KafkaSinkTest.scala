package com.el.flink.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @author roman zhangfei
 * @Date 2020/1/16
 * @Version V1.0
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream = env.readTextFile("in/sensor.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

//    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // Transform操作
    val dataStream = inputStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toInt, dataArray(2).trim.toDouble).toString
      }
    )

    dataStream.addSink(new FlinkKafkaProducer011[String]("flinkTest",new SimpleStringSchema(),properties))
    dataStream.print()

    env.execute("kafka flink test")

  }
}
