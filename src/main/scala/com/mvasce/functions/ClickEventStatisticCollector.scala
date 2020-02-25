package com.mvasce.functions

import com.mvasce.records.ClickEventStatistics
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.Instant

/**
  * Process window function collecting Click events and returning
  * the count
  *
  */
class ClickEventStatisticCollector
    extends ProcessWindowFunction[Int, ClickEventStatistics, String, TimeWindow] {
  override def process(
      key: String,
      context: Context,
      elements: Iterable[Int],
      out: Collector[ClickEventStatistics]
  ): Unit = {
    val count = elements.iterator.next()
    val start = Instant.ofEpochMilli(context.window.getStart).toDateTime
    val end = Instant.ofEpochMilli(context.window.getEnd).toDateTime
    val stat = ClickEventStatistics(start, end, key, count)
    out.collect(stat)
  }
}
