package utils

import types.STPoint

object MobilityChecker {
  def getHaversineDistance(lat1:Double,lon1:Double,lat2:Double,lon2:Double): Double = {
    val R = 6371000; // Radius of the earth in km
    val dLat = deg2rad(lat2-lat1) // deg2rad below
    val dLon = deg2rad(lon2-lon1)
    val a =
      Math.sin(dLat/2) * Math.sin(dLat/2) +
        Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
          Math.sin(dLon/2) * Math.sin(dLon/2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    val d = R * c; // Distance in km
    d
  }


  def getEuclidean(lat1:Double,lon1:Double,lat2:Double,lon2:Double): Double = {
    Math.sqrt(Math.pow(lat1-lat2,2)+Math.pow(lon1-lon2,2))
  }

  def deg2rad(deg:Double):Double ={
    deg * (Math.PI/180)
  }

  def getSpeedKnots(oldLoc: STPoint, newLoc: STPoint): Double = {
    if (newLoc.timestamp > oldLoc.timestamp)                                                                  //timestamp values expressed in milliseconds
      (3600000.0D * getHaversineDistance(oldLoc.latitude, oldLoc.longitude, newLoc.latitude, newLoc.longitude)) / (1852.0D * (newLoc.timestamp - oldLoc.timestamp)) //Return value in knots: nautical miles per hour
    else
      -1.0D            //Placeholder for NULL speed
  }


  def getBearing(oldLoc: STPoint, newLoc: STPoint): Double = {
    val y = Math.sin(Math.toRadians(newLoc.longitude) - Math.toRadians(oldLoc.longitude)) * Math.cos(Math.toRadians(newLoc.latitude))
    val x = Math.cos(Math.toRadians(oldLoc.latitude)) * Math.sin(Math.toRadians(newLoc.latitude)) - Math.sin(Math.toRadians(oldLoc.latitude)) * Math.cos(Math.toRadians(newLoc.latitude)) * Math.cos(Math.toRadians(newLoc.longitude) - Math.toRadians(oldLoc.longitude))
    val bearing = (Math.atan2(y, x) + 2 * Math.PI) % (2 * Math.PI)
    Math.toDegrees(bearing)          //Return angle value between 0 and 359 degrees
  }

  def getBearing(oldLon:Double, oldLat:Double, newLon:Double, newLat:Double): Double = {
    val y = Math.sin(Math.toRadians(newLon) - Math.toRadians(oldLon)) * Math.cos(Math.toRadians(newLat))
    val x = Math.cos(Math.toRadians(oldLat)) * Math.sin(Math.toRadians(newLat)) - Math.sin(Math.toRadians(oldLat)) * Math.cos(Math.toRadians(newLat)) * Math.cos(Math.toRadians(newLon) - Math.toRadians(oldLon))
    val bearing = (Math.atan2(y, x) + 2 * Math.PI) % (2 * Math.PI)
    Math.toDegrees(bearing)          //Return angle value between 0 and 359 degrees
  }

}
