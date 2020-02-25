package com.mvasce.functions

import com.mvasce.records.ClickEvent
import org.apache.flink.api.common.functions.AggregateFunction

//class CountingAggregator extends AggregateFunction[ClickEvent,Long,Long] {
//    override def createAccumulator(): Long = 0L
//    override def add(value: ClickEvent, accumulator: Long): Long = {
//        accumulator + 1
//    }
//    override def getResult(accumulator: Long): Long = accumulator
//    override def merge(x: Long, y: Long): Long = x+y
//}


class CountingAggregator extends AggregateFunction[ClickEvent, Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(value: ClickEvent, accumulator: Int): Int = accumulator + 1

    override def getResult(accumulator: Int): Int = accumulator

    override def merge(a: Int, b: Int): Int = a + b
}