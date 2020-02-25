package com.mvasce

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.{DateTime, Instant}

object Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val hostname: String = "localhost"
    val port: Int = 9000

    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    val mapped: DataStream[Data] = text.map(x => {
      val splitted = x.replace(" ", "").split(",")
      Data(splitted(0), splitted(1).toInt)
    })

    val keyedStream: WindowedStream[Data, String, TimeWindow] =
      mapped.keyBy(_.key).timeWindow(Time.seconds(10))

    val aggregated: DataStream[DataAverage] =
      keyedStream.aggregate(new AverageAggregate, new ProcessFunction)

    aggregated.print()

    env.execute()

  }

  class AverageAggregate extends AggregateFunction[Data, (Int, Int), Double] {
    override def createAccumulator(): (Int, Int) = (0, 0)

    override def add(value: Data, accumulator: (Int, Int)): (Int, Int) =
      (accumulator._1 + value.items, accumulator._2 + 1)

    override def getResult(accumulator: (Int, Int)): Double =
      accumulator._1 / accumulator._2

    override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) =
      (a._1 + b._1, a._2 + b._2)
  }

  class ProcessFunction
      extends ProcessWindowFunction[Double, DataAverage, String, TimeWindow] {
    override def process(
        key: String,
        context: Context,
        elements: Iterable[Double],
        out: Collector[DataAverage]
    ): Unit = {
      val average = elements.iterator.next()
      val start = Instant.ofEpochMilli(context.window.getStart).toDateTime
      val end = Instant.ofEpochMilli(context.window.getEnd).toDateTime
      out.collect(DataAverage(key, average, start, end))

    }
  }

  case class Data(key: String, items: Int)

  case class DataAverage(
      key: String,
      mean_items: Double,
      start: DateTime,
      end: DateTime
  )

}
