package com.mvasce

import org.apache.flink.api.java.utils.ParameterTool
import java.util.Properties
import com.mvasce.records.ClickEventSerializationSchema
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import scala.collection.immutable.HashMap
import com.mvasce.records.ClickEvent
import org.joda.time.DateTime
import org.joda.time.Instant
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.typesafe.sslconfig.util.LoggerFactory

object ClickEventProducer {
  val EVENTS_PER_WINDOW: Int = 1000
  val WINDOWS_SIZE: Long = 100L
  val pages = List("/help", "/index", "/shop", "/jobs", "/about", "/news")

  @volatile var keepRunning = true

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val topic = params.get("topic", "input")
    val kafkaProps = createkafkaParameters(params)

    // this is for stopping gracefully the producer
    val mainThread = Thread.currentThread();
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run = {
        logger.info("Stopping producer and closing kafka connection")
        keepRunning = false
        mainThread.join()
      }
    })

    val iterator = new ClickIterator()
    val producer = new KafkaProducer[String, ClickEvent](kafkaProps)
    while (keepRunning) {
      val event = iterator.next()
      logger.info(s"Sending message $event")
      val record = new ProducerRecord(topic, "key", event)
      producer.send(record)
      Thread.sleep(100)
    }
    producer.close()
  }

  /**
    * Create Kafka producer parameters
    *
    * @param params app arguments
    * @return kafak properties
    */
  def createkafkaParameters(params: ParameterTool): Properties = {
    val brokers = params.get("bootstrap.servers", "localhost:9092")
    val kafkaProps = new Properties()
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    kafkaProps.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )
    kafkaProps.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[ClickEventSerializationSchema]
    )
    kafkaProps
  }

  /**
    * Generate click events
    *
    */
  class ClickIterator {
    import scala.collection.mutable.Map
    private val nextTimeStampPerKey: Map[String, Long] = Map()
    private var nextPageIndex = 0

    private def nextTimeStamp(page: String): DateTime = {
    //   val nextTimeStamp = nextTimeStampPerKey.getOrElse(page, 0L)
    //   nextTimeStampPerKey.put(page, nextTimeStamp + WINDOWS_SIZE)
    //   Instant.ofEpochMilli(nextTimeStamp).toDateTime()
        DateTime.now()
    }

    private def nextPage(): String = {
      val page: String = pages(nextPageIndex)
      nextPageIndex =
        if (nextPageIndex == pages.length - 1) 0 else nextPageIndex + 1
      page
    }

    /**
      * Get the next event
      *
      * @return next click event
      */
    def next(): ClickEvent = {
      val page = nextPage()
      val ts = nextTimeStamp(page)
      ClickEvent(ts, page)
    }
  }

}
