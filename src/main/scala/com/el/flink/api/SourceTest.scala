package com.el.flink.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * @author roman zhangfei
 * @Date 2020/1/15
 * @Version V1.0
 */
// 定义传感器数据样例类
case class SensorReading(id:String,timestamp:Long,tempaerature:Double)

object SourceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //从集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    stream1.print("stream1:")
    val stream2 = env.readTextFile("in/sensor.txt")

    stream2.print("stream2:")

    val flatMap = stream2.flatMap(_.split(",")).map((_,1))
        .startNewChain()
    flatMap.print("flatMap:")

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    stream3.print("stream3:")

    val stream4 = env.addSource(new SensorSource())

    stream4.print("stream4")

    env.execute("source api test")
  }

}

class SensorSource () extends SourceFunction[SensorReading]{

  //定义一个flag: 表示数据源是否还在正常运行
  var running:Boolean = true


  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //创建一个随机数
    val rand = new Random()

    // 随机初始换生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp = (1 to 10).map(
      i => ("sensor_"+i, 60 + rand.nextGaussian() * 20)
    )

    //无限循环生成流数据，除非被cancel
    while (running){
      curTemp = curTemp.map(
        t => (t._1,t._2 + rand.nextGaussian())
      )

      //获取当前的时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )

      //间隔100ms
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = false
}