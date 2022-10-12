package flink.scala.demos

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object _01_入门程序WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream = env
      .socketTextStream("hadoop01", 9998)

//    sourceStream.flatMap(s=>s.split("\\s+")).map { w =>
//      (w, 1)
//    }
    sourceStream
      .flatMap(s => {
        s.split("\\s+").map(w=>(w,1))
      })
      .keyBy(tp => tp._1)
      .sum(1)
      .print("测试")

    env.execute("我的job");

  }

}
