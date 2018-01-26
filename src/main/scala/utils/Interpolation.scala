package utils


import org.apache.commons.math3.analysis.interpolation.{LinearInterpolator, SplineInterpolator}
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction
import org.apache.commons.math3.exception.{NonMonotonicSequenceException, NumberIsTooSmallException}
import points.AvroPoint

object Interpolation {
  def interpolation2D( trajectory:Array[AvroPoint], s:Int) : Array[AvroPoint] = {

    val m = Math.floor((trajectory.last.getTimestamp - trajectory.head.getTimestamp) / s.toDouble).toInt + 1

    val fixedsampling_trajectory: Array[AvroPoint] = new Array[AvroPoint](m)


    val lon_arr: Array[Double] = new Array[Double](trajectory.length)
    val lat_arr: Array[Double] = new Array[Double](trajectory.length)

    val t_arr:Array[Double]=new Array[Double](trajectory.length)

    val t_head:Long=trajectory.head.getTimestamp

    var i=0
    while (i<trajectory.length) {
      lon_arr(i) = trajectory(i).getLongitude
      lat_arr(i) = trajectory(i).getLatitude
      t_arr(i) = trajectory(i).getTimestamp - t_head
      i+=1
    }

    var step: Double = t_arr.head + s

    val point: AvroPoint = new AvroPoint()
    point.setId(trajectory.head.getId)
    point.setTimestamp(trajectory.head.getTimestamp)
    point.setLongitude(trajectory.head.getLongitude)
    point.setLatitude(trajectory.head.getLatitude)
    point.setSpeed(trajectory.head.getSpeed)
    point.setHeading(trajectory.head.getHeading)
    fixedsampling_trajectory(0) = point
    val interp: LinearInterpolator = new LinearInterpolator
    try {
      val lon_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lon_arr)
      val lat_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lat_arr)

      for (i <- 1 until m - 1) {


        val point: AvroPoint = new AvroPoint()
        point.setId(trajectory.head.getId)
        point.setTimestamp(((step * 1000) + t_head).toLong)

        point.setLongitude(lon_psf.value(step))
        point.setLatitude(lat_psf.value(step))

        fixedsampling_trajectory(i) = point
        step = step + s
      }

      val point2: AvroPoint = new AvroPoint()
      point2.setId(trajectory.head.getId)
      point2.setTimestamp((step  + t_head).toLong)
      point2.setLongitude(trajectory.last.getLongitude)
      point2.setLatitude(trajectory.last.getLatitude)

      fixedsampling_trajectory(m - 1) = point2

      fixedsampling_trajectory
    } catch {
      case ioe: NonMonotonicSequenceException => {
        println(ioe.toString)
        System.exit(1)
        null
      }
      case ex: NumberIsTooSmallException => {
        trajectory
      }
    }
  }

  def splinepolation2D(trajectory: Array[AvroPoint], s: Int): Array[AvroPoint] = {


    val m = Math.floor((trajectory.last.getTimestamp - trajectory.head.getTimestamp) / s.toDouble).toInt + 1
    val fixedsampling_trajectory: Array[AvroPoint] = new Array[AvroPoint](m)

    val lon_arr: Array[Double] = new Array[Double](trajectory.length)
    val lat_arr: Array[Double] = new Array[Double](trajectory.length)
    val t_arr: Array[Double] = new Array[Double](trajectory.length)

    val t_head: Long = trajectory.head.getTimestamp

    var i=0
    while (i<trajectory.length) {
      lon_arr(i) = trajectory(i).getLongitude
      lat_arr(i) = trajectory(i).getLatitude
      t_arr(i) = trajectory(i).getTimestamp - t_head
      i+=1
    }

    var step: Double = t_arr.head + s

    val point: AvroPoint = new AvroPoint()
    point.setId(trajectory.head.getId)
    point.setTimestamp(trajectory.head.getTimestamp)
    point.setLongitude(trajectory.head.getLongitude)
    point.setLatitude(trajectory.head.getLatitude)
    point.setSpeed(trajectory.head.getSpeed)
    point.setHeading(trajectory.head.getHeading)

    fixedsampling_trajectory(0) = point
    val interp: SplineInterpolator = new SplineInterpolator
    val asi: LinearInterpolator = new LinearInterpolator

    try {
      val lon_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lon_arr)
      val lat_psf: PolynomialSplineFunction = interp.interpolate(t_arr, lat_arr)

      for (i <- 1 until m - 1) {

        val point: AvroPoint = new AvroPoint()
        point.setId(trajectory.head.getId)
        point.setTimestamp((step + t_head).toLong)
        point.setLongitude(lon_psf.value(step))
        point.setLatitude(lat_psf.value(step))
        fixedsampling_trajectory(i) = point
        step = step + s
      }

      val point2: AvroPoint = new AvroPoint()
      point2.setId(trajectory.head.getId)
      point2.setTimestamp((step + t_head).toLong)
      point2.setLongitude(trajectory.last.getLongitude)
      point2.setLatitude(trajectory.last.getLatitude)

      point2.setSpeed(MobilityChecker.getSpeedKnots(trajectory(trajectory.length-2), trajectory.last))
      fixedsampling_trajectory(m - 1) = point2
      fixedsampling_trajectory

    } catch {
      case ioe: NonMonotonicSequenceException => {
        //println(ioe.toString)
        System.exit(1)
        null
      }
      case ex: NumberIsTooSmallException => {
        trajectory
      }
    }
  }
}
