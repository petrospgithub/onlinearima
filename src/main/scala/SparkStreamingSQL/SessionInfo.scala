package sql.streaming

/**
  * User-defined data type for storing a session information as state in mapGroupsWithState.
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
case class SessionInfo(
                        numEvents: Int,
                        startTimestampMs: Long,
                        endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}