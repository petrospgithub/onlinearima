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

/*
  //Time elapsed between the current and the previously reported location
  /*
  def getElapsedTime(oldLoc: MaritimePoint, newLoc: MaritimePoint): Long = {
    newLoc.getTs - oldLoc.getTs      //Return interval value in MILLISECONDS
  }
  */

  /*
      //Distance between two locations in the Euclidean plane; value is in the same units as the coordinate system of the input locations (i.e., decimal degrees)
      def getEuclideanDistance(oldLoc: critical_point, newLoc: critical_point): Double = {
        Math.sqrt((newLoc.getLongitude - oldLoc.getLongitude) * (newLoc.getLongitude - oldLoc.getLongitude) + (newLoc.getLatitude - oldLoc.getLatitude) * (newLoc.getLatitude - oldLoc.getLatitude)) //in decimal degrees
      }
  */

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

  def getHaversineDistance(oldLoc: PredictedPoint, newLoc: PointST): Double = {
    val deltaLat = Math.toRadians(newLoc.getLatitude - oldLoc.lat)
    val deltaLon = Math.toRadians(newLoc.getLongitude - oldLoc.lon)
    val a = Math.pow(Math.sin(deltaLat / 2.0D), 2) + Math.cos(Math.toRadians(newLoc.getLatitude)) * Math.cos(Math.toRadians(oldLoc.lat)) * Math.pow(Math.sin(deltaLon / 2.0D), 2)
    val greatCircleDistance = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a))
    //3958.761D * greatCircleDistance           //Return value in miles
    //3440.0D * greatCircleDistance             //Return value in nautical miles
    6371000.0D * greatCircleDistance            //Return value in meters, assuming Earth radius is 6371 km
  }

  /*
      //Calculate speed of movement (in meters/sec) from one location to another
      def getSpeed(oldLoc: critical_point, newLoc: critical_point): Double = {
        if (newLoc.getTimestamp > oldLoc.getTimestamp)                                                 //timestamps in milliseconds
          1000.0D * getHaversineDistance(oldLoc, newLoc) / (newLoc.getTimestamp - oldLoc.getTimestamp) //Return value in meters/sec
        else
          -1.0D           //Placeholder for NULL speed
      }
  */

  //Calculate speed of movement (in knots) from one location to another
  def getSpeedKnots(oldLoc: PointST, newLoc: PointST): Double = {
    if (newLoc.getTimestamp > oldLoc.getTimestamp)                                                                  //timestamp values expressed in milliseconds
      (3600000.0D * getHaversineDistance(oldLoc, newLoc)) / (1852.0D * (newLoc.getTimestamp - oldLoc.getTimestamp)) //Return value in knots: nautical miles per hour
    else
      -1.0D            //Placeholder for NULL speed
  }

  //Calculate the azimuth (relative to North) between two locations
  def getBearing(oldLoc: PointST, newLoc: PointST): Double = {
    val y = Math.sin(Math.toRadians(newLoc.getLongitude) - Math.toRadians(oldLoc.getLongitude)) * Math.cos(Math.toRadians(newLoc.getLatitude))
    val x = Math.cos(Math.toRadians(oldLoc.getLatitude)) * Math.sin(Math.toRadians(newLoc.getLatitude)) - Math.sin(Math.toRadians(oldLoc.getLatitude)) * Math.cos(Math.toRadians(newLoc.getLatitude)) * Math.cos(Math.toRadians(newLoc.getLongitude) - Math.toRadians(oldLoc.getLongitude))
    val bearing = (Math.atan2(y, x) + 2 * Math.PI) % (2 * Math.PI)
    Math.toDegrees(bearing)          //Return angle value between 0 and 359 degrees
  }


  //Calculates the angular difference (in degrees) between two given headings (azimuth values)
  def angleDifference(heading1: Double, heading2: Double): Double = {
    val phi: Double = Math.abs(heading1 - heading2) % 360
    if (phi > 180)
      360.0D - phi            //Return angle value between 0 and 359 degrees
    else
      phi
  }

  //Get slope difference between two angles (in degrees) in the trigonometric cycle
  //This returns values with a sign (+/-)
  def slopeDifference(heading1: Double, heading2: Double): Double = {
    180.0D - Math.abs(180.0D - (heading2 - heading1))               //Return angle value between -180 and 180 degrees
  }

  //Calculate acceleration (sign: +) or deceleration (sign: -) over ground; speed and elapsed time values must have been calculated beforehand for each location
  def getRateOfChangeKnots(oldLoc: PointST, newLoc: PointST): Double = {
    if ((newLoc.getTimestamp-oldLoc.getTimestamp) > 0L)
      (3600000.0D * (newLoc.getSpeed - oldLoc.getSpeed)) / (1.0D * (newLoc.getTimestamp-oldLoc.getTimestamp))    //Return value in knots/hour; speed has been already calculated in knots
    else
      0.0D;                   //Value cannot be calculated
  }

  //Calculates rate of turn (in degrees/sec) between two given locations; actually the change in heading (angle in azimuth values) between those two sample locations
  def getRateOfTurn(newLoc: PointST, oldLoc: PointST): Double = {
    //val phi: Double = (Math.abs(newLoc.getHeading - oldLoc.getHeading + 180)) % 360 - 180
    val a: Double = newLoc.getHeading - oldLoc.getHeading
    val phi: Double = ((a + 180) % 360 + 360) % 360 - 180               //Workaround in order to return a positive difference of angles

    if (phi > 180)
      (360.0D - phi) / (0.001D * (newLoc.getTimestamp-oldLoc.getTimestamp))      //Return value in degrees/sec
    else
      phi / (0.001D * (newLoc.getTimestamp-oldLoc.getTimestamp))
  }

  def eliminateNoise(oldLoc: PointST, newLoc: PointST,
                     MAX_SPEED_THRESHOLD:Double
                    ): Boolean = {

    var isNoise = false                    //Flag: no location qualifies as noise beforehand

    //First, in case of NULL or EXCESSIVE speed, this location qualifies for noise
    if ((newLoc.getSpeed < 0.0F) || (newLoc.getSpeed >= MAX_SPEED_THRESHOLD)) {
      isNoise = true
    }

    isNoise            //Return flag: whether this location qualifies as noise or not
  }
  */
}