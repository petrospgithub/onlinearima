package types

case class STPoint(id:Int, timestamp:Long, longitude:Double, latitude:Double, var speed:Double, var heading:Double, var error:Boolean)
