package sql.streaming

import java.sql.Timestamp

/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)
