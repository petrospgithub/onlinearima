package utils

import org.apache.commons.math3.analysis.interpolation.{LinearInterpolator, SplineInterpolator}
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction
import org.apache.commons.math3.exception.{NonMonotonicSequenceException, NumberIsTooSmallException}
import types.STPoint

object Interpolation {
  def splinepolation2D(trajectory: Array[STPoint], s: Int): Array[STPoint] = {


    val m = Math.floor((trajectory.last.timestamp - trajectory.head.timestamp) / s.toDouble).toInt + 1
    val fixedsampling_trajectory: Array[STPoint] = new Array[STPoint](m)

    val lon_arr: Array[Double] = new Array[Double](trajectory.length)
    val lat_arr: Array[Double] = new Array[Double](trajectory.length)
    val t_arr: Array[Double] = new Array[Double](trajectory.length)

    val t_head: Long = trajectory.head.timestamp

    var i=0
    while (i<trajectory.length) {
      lon_arr(i) = trajectory(i).longitude
      lat_arr(i) = trajectory(i).latitude
      t_arr(i) = trajectory(i).timestamp - t_head
      i+=1
    }

    var step: Double = t_arr.head + s
/*
    val point: AvroPoint = new AvroPoint()
    point.setId(trajectory.head.getId)
    point.setTimestamp(trajectory.head.getTimestamp)
    point.setLongitude(trajectory.head.getLongitude)
    point.setLatitude(trajectory.head.getLatitude)
    point.setSpeed(trajectory.head.getSpeed)
    point.setHeading(trajectory.head.getHeading)
    */
    val point=STPoint(
      trajectory.head.id,
      trajectory.head.timestamp,
      trajectory.head.longitude,
      trajectory.head.latitude, 0, 0, error = false
    )

    fixedsampling_trajectory(0) = point
    val interp: SplineInterpolator = new SplineInterpolator
    val asi: LinearInterpolator = new LinearInterpolator

    try {
      val lon_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lon_arr)
      val lat_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lat_arr)

      for (i <- 1 until m - 1) {

        val point=STPoint(
          trajectory.head.id,
          (step + t_head).toLong,
          lon_psf.value(step),
          lat_psf.value(step), 0, 0, error = false
        )

        fixedsampling_trajectory(i) = point
        step = step + s
      }

      val point2=STPoint(
        trajectory.head.id,
        (step + t_head).toLong,
        trajectory.last.longitude,
        trajectory.last.latitude,0,0,error = false
      )

      //point2.setSpeed(MobilityChecker.getSpeedKnots(trajectory(trajectory.length-2), trajectory.last))
      fixedsampling_trajectory(m - 1) = point2
      fixedsampling_trajectory

    } catch {
      case ioe: NonMonotonicSequenceException =>
        //println(ioe.toString)
        //System.exit(1)
        Array[STPoint]()
      case ex: NumberIsTooSmallException =>
        Array[STPoint]()
    }
  }
}
