package com.mvasce.records

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json.Format
import org.joda.time.DateTime
import play.api.libs.json.JodaReads
import play.api.libs.json.JodaWrites
import play.api.libs.json.Json

class ClickEventSerializationSchema(val topic: String) extends KafkaSerializationSchema[ClickEvent]{
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    implicit val dateFormat = Format[DateTime](
        JodaReads.jodaDateReads(pattern),
        JodaWrites.jodaDateWrites(pattern)
    )
    implicit val clickEventReader = Json.format[ClickEvent]

    override def serialize(element: ClickEvent, timestamp: java.lang.Long): ProducerRecord[Array[Byte],Array[Byte]] = {
        new ProducerRecord(topic, Json.toJson(element).toString().getBytes())
    }
}