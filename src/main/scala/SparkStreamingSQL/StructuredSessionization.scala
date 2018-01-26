package sql.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
/* * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredSessionization
 * localhost 9999`
 */

object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

        // If timed out, then remove session and send final update
        if (state.hasTimedOut) {
          val finalUpdate =
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          // Update start and end timestamps in session
          val timestamps = events.map(_.timestamp.getTime).toSeq
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("10 seconds")
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}






