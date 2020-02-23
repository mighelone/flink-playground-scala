package com.mvasce


import com.mvasce.records.ClickEvent
import org.joda.time.Instant
import org.joda.time.DateTime

object Test {
  def main(args: Array[String]): Unit = {
      val timestamp = new DateTime(2020, 2, 23, 20, 0)
    // val ts =  Instant.ofEpochMilli(2143535553L)
    // val date = Date.from(ts)

    val event = ClickEvent(timestamp, "something")
    println(event)

  }
}
