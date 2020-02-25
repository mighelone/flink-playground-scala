package com.mvasce

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import com.mvasce.records.ClickEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
// import org.apache.flink.streaming.api.scala.function
import org.apache.flink.api.common.functions.AggregateFunction

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import com.mvasce.records.ClickEvent
import com.mvasce.functions.CountingAggregator
import com.mvasce.functions.ClickEventStatisticCollector
import com.mvasce.records.ClickEventStatistics

object ClickEventCounter {

  val CHECKPOINTING_OPTION = "checkpointing"
  val ENVENTIME_OPTION = "event-time"
  val BACKPRESSURE_OPTION = "backpressure"
  val WINDOW_SIZE = Time.seconds(10) // Time.of(15, TimeUnit.SECONDS)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val inflictBackpressure: Boolean = params.has(BACKPRESSURE_OPTION)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    configureEnvironment(params, env)

    val inputTopic = params.get("input-topic", "input")
    val outputTopic = params.get("output-topic", "output")
    val brokers = params.get("bootstrap.servers", "localhost:9092")
    val kafkaProps = new Properties
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count")

    val clicks: DataStream[ClickEvent] = env
      .addSource(
        new FlinkKafkaConsumer(
          inputTopic,
          new ClickEvent.ClickEventDeserializationSchema,
          kafkaProps
        )
      )
      .name("ClickEvent source")
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ClickEvent](
          Time.seconds(200)
        ) {
          override def extractTimestamp(element: ClickEvent): Long =
            element.getMillis()
        }
      )

    if (inflictBackpressure) {
      println("Not implemented")
    }

    val windowStream: WindowedStream[ClickEvent, String, TimeWindow] = clicks
      .keyBy((x: ClickEvent) => x.page)
      .timeWindow(WINDOW_SIZE)

    val statistics: DataStream[ClickEventStatistics] = windowStream
      .aggregate(new CountingAggregator, new ClickEventStatisticCollector)

    statistics.print()

    env.execute("ClickEventCounter")

  }

  /**
    * Configure Flink environment
    *
    * @param params CLI parameters
    * @param env Flink Environment context
    */
  def configureEnvironment(
      params: ParameterTool,
      env: StreamExecutionEnvironment
  ): Unit = {
    val checkpointingEnabled: Boolean = params.has(CHECKPOINTING_OPTION)
    val eventTimeSemantics: Boolean = params.has(ENVENTIME_OPTION)

    if (checkpointingEnabled) env.enableCheckpointing(1000)

    if (eventTimeSemantics)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // TODO this option produces errors
    //disabling Operator chaining to make it easier to follow the Job in the WebUI
    // env.disableOperatorChaining()
  }
}
