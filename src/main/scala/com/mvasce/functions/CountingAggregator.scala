package com.mvasce.functions

import com.mvasce.records.ClickEvent
import org.apache.flink.api.common.functions.AggregateFunction



class CountingAggregator extends AggregateFunction[ClickEvent, Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(value: ClickEvent, accumulator: Int): Int = accumulator + 1

    override def getResult(accumulator: Int): Int = accumulator

    override def merge(a: Int, b: Int): Int = a + b
}