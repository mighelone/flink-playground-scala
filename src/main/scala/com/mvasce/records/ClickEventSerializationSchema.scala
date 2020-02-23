package com.mvasce.records

// import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json.Format
import org.joda.time.DateTime
import play.api.libs.json.JodaReads
import play.api.libs.json.JodaWrites
import play.api.libs.json.Json
import java.util

class ClickEventSerializationSchema extends Serializer[ClickEvent]{
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    implicit val dateFormat = Format[DateTime](
        JodaReads.jodaDateReads(pattern),
        JodaWrites.jodaDateWrites(pattern)
    )
    implicit val clickEventReader = Json.format[ClickEvent]

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, element: ClickEvent): Array[Byte] = {
        Json.toJson(element).toString().getBytes()
    }

    override def close(): Unit = {}
}
