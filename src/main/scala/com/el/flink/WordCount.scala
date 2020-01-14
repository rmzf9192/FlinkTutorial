package com.el.flink

import org.apache.flink.api.scala._



/**
 * @author roman zhangfei
 * @Date 2020/1/14
 * @Version V1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "in/hello.txt"
    val inputDS:DataSet[String] = env.readTextFile(inputPath)

    // 分词之后做count
    val wordCountDataSet:AggregateDataSet[(String,Int)] = inputDS.flatMap(_.split(" "))
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
