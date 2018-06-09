package evaluation

import java.sql.{Connection, DriverManager, ResultSet}

class PostgresBroadcast(createConnection: () => Connection) extends Serializable {
  lazy val connection = createConnection()
  def select(sql:String, id:Int, t:Long): ResultSet = {
    try {
      val prep=connection.prepareStatement(sql)

      prep.setInt(1, id)
      prep.setLong(2, t)

      prep.executeQuery()

    } catch {
      case e: Exception => null
    }
  }
  def insert(sql:String, settings:String, timestamp:Long, horizon:Int, dist:Double, alt:Double): Unit = {
    try {
      val prep=connection.prepareStatement(sql)

      prep.setString(1, settings)
      prep.setLong(2, timestamp)
      prep.setInt(3, horizon)
      prep.setDouble(4, dist)
      prep.setDouble(5, alt)

      prep.execute()

    } catch {
      case e: Exception => null
    }
  }
}

object PostgresBroadcast {
  //TODO
  def apply(url:String, user:String, pass:String): PostgresBroadcast = {
    val createFunc = () => {
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(url, user, pass)

      //val stmt = connection.createStatement()
      //stmt.execute(sql)
      sys.addShutdownHook {
        connection.close()
      }

      connection
    }
    new PostgresBroadcast(createFunc)
  }
}
