package com.el.flink.api

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.types.FlinkScalaKryoInstantiator
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author roman zhangfei
 * @Date 2020/1/15
 * @Version V1.0
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val inputStream = env.readTextFile("in/sensor.txt")

    val dataStream = inputStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).toDouble)
      }
    )

    //聚合操作
    val stream1 = dataStream.keyBy("id")
      //      .sum("temperature")
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.tempaerature + 10))
    stream1.print("stream1:")
    //分流，根据温度是否大于30度划分
    val splitStream = dataStream.split(
      sensorData => {
        if (sensorData.tempaerature > 30) Seq("high") else Seq("lows")
      }
    )
    splitStream.print("splitStream:")
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    val union = highTempStream.union(lowTempStream).map(_.tempaerature.toString)

    // 主函数中添加 sink
    union.addSink(new FlinkKafkaProducer011[String]("localhost:9092","test",new SimpleStringSchema()))

    // 3. 合并两条流
    val warningStream = highTempStream.map( sensorData => (sensorData.id, sensorData.tempaerature) )
    warningStream.print("warningStream:")
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => ( warningData._1, warningData._2, "high temperature warning" ),
      lowData => ( lowData.id, "healthy" )
    )
    coMapStream.print("coMapStream:")
    val unionStream = highTempStream.union(lowTempStream)
    unionStream.print("unionStream:")

    dataStream.filter(new MyFliter()).print()

    // 输出数据
    //    dataStream.print()
    //    highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")
    //    unionStream.print("union")

    dataStream.map(new MyMapper()).print()

    env.execute("transform test job")
  }
}
class MyFliter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyMapper() extends  RichMapFunction[SensorReading,String]{
  override def map(in: SensorReading): String = {
    "flink"
  }
  override def open(parameters: Configuration): Unit = super.open(parameters)
}