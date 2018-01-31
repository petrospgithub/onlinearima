package SparkStreamingSQL

import java.lang

import points.PointST

case class Point(object_id:Int, timestamp:Long, longitude:Double, latitude:Double, var speed:Double, var heading:Double) extends PointST {
  override def getId: Integer = object_id

  override def getLongitude: lang.Double = longitude

  override def getLatitude: lang.Double = latitude

  override def getSpeed: lang.Double = speed

  override def getHeading: lang.Double = heading

  override def getTimestamp: lang.Long = timestamp

  override def setSpeed(value: lang.Double): Unit = this.speed=value

  override def setHeading(value: lang.Double): Unit = this.heading=value
}

