package com.mvasce.records

import org.joda.time.DateTime

case class ClickEvent(
    timestamp: DateTime,
    page: String
)
