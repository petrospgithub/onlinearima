package sql.streaming

/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  * @param id          Id of the session
  * @param durationMs  Duration the session was active, that is, from first event to its expiry
  * @param numEvents   Number of events received by the session while it was active
  * @param expired     Is the session active or expired
  */
case class SessionUpdate(
                          id: String,
                          durationMs: Long,
                          numEvents: Int,
                          expired: Boolean)
