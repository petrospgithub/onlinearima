package types

case class STPoint(id:Int, timestamp:Long, var longitude:Double, var latitude:Double, var speed:Double, var heading:Double, var error:Boolean) {
  override def toString: String = {
    "{id: %s, timestamp: %s, longitude: %s, latitude:%s, speed: %s, heading: %s}".format(id,timestamp,longitude,latitude,speed,heading)
  }
}
