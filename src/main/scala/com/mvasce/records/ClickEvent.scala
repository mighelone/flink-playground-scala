package com.mvasce.records

import org.joda.time.DateTime
import play.api.libs.json.Format
import play.api.libs.json.JodaReads
import play.api.libs.json.JodaWrites
import play.api.libs.json.Json
import org.apache.flink.streaming.util.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

case class ClickEvent(
    timestamp: DateTime,
    page: String
)

object ClickEvent {
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    implicit val dateFormat = Format[DateTime](
        JodaReads.jodaDateReads(pattern),
        JodaWrites.jodaDateWrites(pattern)
    )
    implicit val clickEventReader = Json.format[ClickEvent]
    
    class ClickEventDeserializationSchema extends DeserializationSchema[ClickEvent]{
        

        override def deserialize(message: Array[Byte]): ClickEvent = {
            val json = Json.parse(message)
            Json.fromJson[ClickEvent](json).get
        }

        override def getProducedType(): TypeInformation[ClickEvent] = TypeExtractor.getForClass(classOf[ClickEvent])
        override def isEndOfStream(x: ClickEvent): Boolean = false
    }
}