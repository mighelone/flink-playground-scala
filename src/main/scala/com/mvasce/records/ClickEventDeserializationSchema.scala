package com.mvasce.records

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import java.util.Date
import java.time.Instant
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import play.api.libs.json.Format
import org.joda.time.DateTime
import play.api.libs.json.JodaReads
import play.api.libs.json.JodaWrites
import play.api.libs.json.Json

class ClickEventDeserializationSchema extends DeserializationSchema[ClickEvent]{
    val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    implicit val dateFormat = Format[DateTime](
        JodaReads.jodaDateReads(pattern),
        JodaWrites.jodaDateWrites(pattern)
    )
    implicit val clickEventReader = Json.format[ClickEvent]

    override def deserialize(message: Array[Byte]): ClickEvent = {
        val json = Json.parse(message)
        Json.fromJson[ClickEvent](json).get
    }

    override def getProducedType(): TypeInformation[ClickEvent] = TypeExtractor.getForClass(classOf[ClickEvent])
    override def isEndOfStream(x: ClickEvent): Boolean = false
}