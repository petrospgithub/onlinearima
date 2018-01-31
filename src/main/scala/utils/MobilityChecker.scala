package utils

import points.PointST

object MobilityChecker {

  //Approximate Haversine distance between a pair of lon/lat coordinates
  def getHaversineDistance(oldLoc: PointST, newLoc: PointST): Double = {
    val deltaLat = Math.toRadians(newLoc.getLatitude - oldLoc.getLatitude)
    val deltaLon = Math.toRadians(newLoc.getLongitude - oldLoc.getLongitude)
    val a = Math.pow(Math.sin(deltaLat / 2.0D), 2) + Math.cos(Math.toRadians(newLoc.getLatitude)) * Math.cos(Math.toRadians(oldLoc.getLatitude)) * Math.pow(Math.sin(deltaLon / 2.0D), 2)
    val greatCircleDistance = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a))
    //3958.761D * greatCircleDistance           //Return value in miles
    //3440.0D * greatCircleDistance             //Return value in nautical miles
    6371000.0D * greatCircleDistance            //Return value in meters, assuming Earth radius is 6371 km
  }

  def getSpeedKnots(oldLoc: PointST, newLoc: PointST): Double = {
    if (newLoc.getTimestamp > oldLoc.getTimestamp)                                                                  //timestamp values expressed in milliseconds
      (3600000.0D * getHaversineDistance(oldLoc, newLoc)) / (1852.0D * (newLoc.getTimestamp - oldLoc.getTimestamp)) //Return value in knots: nautical miles per hour
    else
      -1.0D            //Placeholder for NULL speed
  }

  def getHaversineDistance(oldLoc: PredictedPoint, newLoc: PointST): Double = {
    val deltaLat = Math.toRadians(newLoc.getLatitude - oldLoc.lat)
    val deltaLon = Math.toRadians(newLoc.getLongitude - oldLoc.lon)
    val a = Math.pow(Math.sin(deltaLat / 2.0D), 2) + Math.cos(Math.toRadians(newLoc.getLatitude)) * Math.cos(Math.toRadians(oldLoc.lat)) * Math.pow(Math.sin(deltaLon / 2.0D), 2)
    val greatCircleDistance = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a))
    //3958.761D * greatCircleDistance           //Return value in miles
    //3440.0D * greatCircleDistance             //Return value in nautical miles
    6371000.0D * greatCircleDistance            //Return value in meters, assuming Earth radius is 6371 km
  }

  //Calculate the azimuth (relative to North) between two locations
  def getBearing(oldLoc: PointST, newLoc: PointST): Double = {
    val y = Math.sin(Math.toRadians(newLoc.getLongitude) - Math.toRadians(oldLoc.getLongitude)) * Math.cos(Math.toRadians(newLoc.getLatitude))
    val x = Math.cos(Math.toRadians(oldLoc.getLatitude)) * Math.sin(Math.toRadians(newLoc.getLatitude)) - Math.sin(Math.toRadians(oldLoc.getLatitude)) * Math.cos(Math.toRadians(newLoc.getLatitude)) * Math.cos(Math.toRadians(newLoc.getLongitude) - Math.toRadians(oldLoc.getLongitude))
    val bearing = (Math.atan2(y, x) + 2 * Math.PI) % (2 * Math.PI)
    Math.toDegrees(bearing)          //Return angle value between 0 and 359 degrees
  }


}