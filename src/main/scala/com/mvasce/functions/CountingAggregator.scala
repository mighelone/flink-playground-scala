package com.mvasce.functions

import org.apache.flink.api.common.functions.AggregateFunction
import com.mvasce.records.ClickEvent

class CountingAggregator extends AggregateFunction[ClickEvent,Long,Long] {
    override def createAccumulator(): Long = 0L
    override def add(value: ClickEvent, accumulator: Long): Long = {
        accumulator + 1
    }
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(x: Long, y: Long): Long = x+y
}