package com.mvasce

import java.util.Properties

import com.mvasce.records.ClickEvent
import com.mvasce.functions.CountingAggregator
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object ClickEventCounter {

  val CHECKPOINTING_OPTION = "checkpointing"
  val ENVENTIME_OPTION = "event-time"
  val BACKPRESSURE_OPTION = "backpressure"
  val WINDOW_SIZE = Time.seconds(15) // Time.of(15, TimeUnit.SECONDS)


  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val inflictBackpressure: Boolean = params.has(BACKPRESSURE_OPTION)

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    configureEnvironment(params, env)

    val inputTopic = params.get("input-topic", "input")
    val outputTopic = params.get("output-topic", "output")
    val brokers = params.get("bootstrap.servers", "localhost:9092")
    val kafkaProps = new Properties
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count")

    val clicks = env
      .addSource(
        new FlinkKafkaConsumer(
          inputTopic,
          new ClickEvent.ClickEventDeserializationSchema,
          kafkaProps
        )
      )
      .name("ClickEvent source")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ClickEvent](Time.seconds(200)) {
        override def extractTimestamp(element: ClickEvent): Long = element.getMillis()
      }) 

    if (inflictBackpressure) {
        println("Not implemented")
    }

    val statistics = clicks
        .keyBy(_.page)
        .timeWindow(WINDOW_SIZE)
        .aggregate(new CountingAggregator)
        // .name("ClickEventStatistics Sink")

    statistics.print()
    // statistics.addSink(new FlinkKafkaProducer[](outputTopic, ))

    env.execute("ClickEventCounter")

  }

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
