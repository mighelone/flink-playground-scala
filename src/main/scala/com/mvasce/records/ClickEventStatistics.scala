package com.mvasce.records

import org.joda.time.DateTime
import play.api.libs.json.Format
import play.api.libs.json.JodaReads
import play.api.libs.json.JodaWrites
import play.api.libs.json.Json
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.flink.api.common.serialization.SerializationSchema

case class ClickEventStatistics(
    windowStart: DateTime,
    windowEnd: DateTime,
    page: String,
    count: Long
)

object ClickEventStatistics {
  val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  implicit val dateFormat = Format[DateTime](
    JodaReads.jodaDateReads(pattern),
    JodaWrites.jodaDateWrites(pattern)
  )
  implicit val clickEventReader = Json.format[ClickEventStatistics]

  class ClickEventStatisticsSerializationSchema
      extends SerializationSchema[ClickEventStatistics] {
    override def serialize(element: ClickEventStatistics): Array[Byte] =
      Json.toJson(element).toString().getBytes()

  }
}
