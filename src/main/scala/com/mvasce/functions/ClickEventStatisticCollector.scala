package com.mvasce.functions

import com.mvasce.records.ClickEventStatistics
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.Instant

//class ClickEventStatisticCollector
//    extends ProcessWindowFunction[
//      Long,
//      ClickEventStatistics,
//      String,
//      TimeWindow
//    ] {
//  override def process(
//      page: String,
//      context: Context,
//      elements: Iterable[Long],
//      out: Collector[ClickEventStatistics]
//  ): Unit = {
//      val count = elements.iterator.next()
//      val start = context.window.getStart()
//      val end = context.window.getEnd()
//      val stat = ClickEventStatistics(
//            Instant.ofEpochMilli(start).toDateTime,
//            Instant.ofEpochMilli(end).toDateTime,
//            page,
//            count)
//
//      out.collect(stat)
//
//  }
//}

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
