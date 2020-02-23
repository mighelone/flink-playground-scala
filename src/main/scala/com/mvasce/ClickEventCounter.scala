package com.mvasce

import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import com.mvasce.records.ClickEvent

object ClickEventCounter {

  val CHECKPOINTING_OPTION = "checkpointing"
  val ENVENTIME_OPTION = "event-time"
  val BACKPRESSURE_OPTION = "backpressure"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    configureEnvironment(params, env)

    val inputTopic = params.get("input-topic", "input")
    val outputTopic = params.get("output-topic", "output")
    val brokers = params.get("bootstrap.servers", "localhost:9092")
    val kafkaProps = new Properties
    kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count")

    val clicks = env.addSource(
      new FlinkKafkaConsumer(
        inputTopic,
        new ClickEvent.ClickEventDeserializationSchema,
        kafkaProps
      )
    )
    clicks.print()

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
