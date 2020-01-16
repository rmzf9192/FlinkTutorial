package com.el.flink.api

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * @author roman zhangfei
 * @Date 2020/1/16
 * @Version V1.0
 */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream = env.readTextFile("in/sensor.txt")

    // transform
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )

    val httpHo = new util.ArrayList[HttpHost]()

    httpHo.add(new HttpHost("localhost",9200))

    val esFlinkStream = new ElasticsearchSink.Builder[SensorReading](
      httpHo,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data:" + t)
          //包装成一个map或者JsonObject
          val json = new util.HashMap[String, String]

          json.put("sensor_id", t.id)
          json.put("temperature", t.tempaerature.toString)
          json.put("ts", t.timestamp.toString)

          val indexReq = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)
          //利用index 发送请求，写入数据
          requestIndexer.add(indexReq)
          println("data saved.")
        }
      }
    )

    dataStream.addSink(esFlinkStream.build())

    env.execute("es sink test")
  }
}
