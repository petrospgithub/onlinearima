package SparkStreamingSQL

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import SparkStreaming.OARIMAstate
import onlinearima.{OARIMA_ogd, RandomInit}
import org.apache.commons.math3.linear.{Array2DRowRealMatrix, MatrixUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import points.AvroPoint
import utils.{Copy, Interpolation, MobilityChecker}

import scala.collection.mutable.ArrayBuffer

object StructuredSessionizationOARIMAGradientDescent {

  //todo error handling
  //todo evaluation

  def main(args: Array[String]): Unit = {

    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/onlinearima_configuration.properties")
    val prop: Properties = new Properties()
    prop.load(input)

    val input_var = Map(
      "predicted_locations" -> prop.getProperty("Horizon").toInt,
      "historical_positions" -> prop.getProperty("window").toInt,
      "sampling" -> prop.getProperty("sampling").toInt
    )

    val threshold_var = Map(
      "GAP_PERIOD" -> prop.getProperty("GAP_PERIOD").toDouble,
      "MAX_SPEED_THRESHOLD" -> prop.getProperty("MAX_SPEED_THRESHOLD").toDouble
    )


    val spark = SparkSession
      .builder
      .appName("StructuredSessionization").master("local[*]")
      .getOrCreate()

    val broadcastVar = spark.sparkContext.broadcast(input_var)
    val threshold_broadcastVar = spark.sparkContext.broadcast(threshold_var)

    val temp=prop.getProperty("l_rate").split("^")

    val lrateVar=spark.sparkContext.broadcast(prop.getProperty("l_rate").toDouble)

    val prediction_counter = spark.sparkContext.longAccumulator("prediction_counter")

    val prediction_error_counter = spark.sparkContext.longAccumulator("prediction_error_counter")

    import spark.implicits._

    val message = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datacron")
      .load().selectExpr("CAST(value AS STRING)")


    val events = message.map { record =>

        val point: Array[String] = record.getAs[String](0).split(";")
        val p: AvroPoint = new AvroPoint()

        p.setId(point(0).toInt)
        p.setTimestamp(point(1).toLong)
        p.setLongitude(point(2).toDouble)
        p.setLatitude(point(3).toDouble)
        p.setSpeed(point(5).toDouble)
        p.setHeading(point(4).toDouble)

        Point.apply(p.getId, p.getTimestamp, p.getLongitude, p.getLatitude, p.getSpeed, p.getHeading)

      }


    def currentState(key: Int, events: Iterator[Point], state: GroupState[Array[Point]]): Iterator[Point] = {
      val h=broadcastVar.value("historical_positions")
      val curr_state:Array[Point]=state.exists match {
        case true => {
          val temp=events.toArray
          val s=state.get

          val new_arr:Array[Point]=new Array[Point](s.length+temp.length)

          var j=0

          while (j<s.length) {
            new_arr(j)=s(j)
            j=j+1
          }

          j=0

          while (j<temp.length) {
            new_arr(j+s.length)=temp(j)
            j=j+1
          }

          new_arr

          OARIMAstate(events.toArray, new Array2DRowRealMatrix, new Array2DRowRealMatrix, w_lon, w_lat, 1, new AvroPoint, new_point)

        }
        case false => {
          val w_lon=RandomInit.create_w(h)
          val w_lat=RandomInit.create_w(h)
          events.toArray
          OARIMAstate(events.toArray, new Array2DRowRealMatrix, new Array2DRowRealMatrix, w_lon, w_lat, 1, new AvroPoint, new_point)

        }
      }

      val prediction_result: ArrayBuffer[Point] = new ArrayBuffer[Point]()

      val sampling = broadcastVar.value("sampling")
      var mode = false

      //var i = 0
      var j = 0
      while (j < curr_state.size - 1 && !mode) {
        val elapsedTime = curr_state(j + 1).timestamp - curr_state(j).timestamp
        if (elapsedTime != sampling) mode = true
        j += 1
      }

      val spline: Array[Point] = mode match {
        case false => Copy.deepCopy(curr_state.sortWith(_.timestamp < _.timestamp)).toArray
        case true => {
          if (Math.floor((curr_state.last.timestamp - curr_state.head.timestamp) / sampling.toDouble).toInt + 1 > h && curr_state.length > 3) {
            Interpolation.splinepolation2D(curr_state.sortWith(_.timestamp < _.timestamp), sampling)
          } else {
            Array.empty
          }
        }
      }

      val spline_length = spline.length
      //var ret = ""
      if (!spline.isEmpty && spline.length >= h) {

        prediction_counter.add(1)
        val trainSet = spline.slice(spline_length - h, spline_length)

        var j=0
        val lon_arr=new Array[Double](trainSet.length)

        val lat_arr=new Array[Double](trainSet.length)
        while (j<trainSet.length) {
          lon_arr(j)=trainSet(j).longitude
          lat_arr(j)=trainSet(j).latitude
        }

        prediction_result += trainSet.last
        var timestamp = trainSet.last.timestamp
        timestamp += sampling

        val ret_ogd_lon=OARIMA_ogd.prediction(lon_arr, curr_state.w_lon)
        val ret_ogd_lat=OARIMA_ogd.prediction(lat_arr, curr_state.w_lat)

        timestamp += sampling

        val predicted_point2=Point(key, timestamp, ret_ogd_lon._1,ret_ogd_lat._1,
          MobilityChecker.getSpeedKnots(prediction_result.last, predicted_point2),
          MobilityChecker.getBearing(prediction_result.last, predicted_point2))

        prediction_result += predicted_point

        curr_state.prediction.setId(key)
        curr_state.prediction.setTimestamp(timestamp)
        curr_state.prediction.setLongitude(predicted_point.getLongitude)
        curr_state.prediction.setLatitude(predicted_point.getLatitude)
        curr_state.prediction.setSpeed(predicted_point.getSpeed)
        curr_state.prediction.setHeading(predicted_point.getHeading)

        curr_state.lon.setRowMatrix(0, MatrixUtils.createRowRealMatrix(lon_arr))
        curr_state.lat.setRowMatrix(0, MatrixUtils.createRowRealMatrix(lat_arr))

        println(ret_ogd_lon._1+" "+ret_ogd_lat._1)

        var predictions=1

        val buf_lon=new Array[Double](h)
        val buf_lat=new Array[Double](h)

        j=1

        while (j<lon_arr.length) {
          buf_lon(j)=lon_arr(j)
          buf_lon(j)=lon_arr(j)
        }

        buf_lon(buf_lon.length-1)=ret_ogd_lon._1
        buf_lat(buf_lat.length-1)=ret_ogd_lat._1

        while (predictions<=broadcastVar.value("Horizon")) {

          val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon).transpose())
          val pred_lat=ret_ogd_lat._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat).transpose())

          println(pred_lon.getEntry(0,0)+" "+pred_lat.getEntry(0,0))
          timestamp += sampling
          val predicted_point2=Point(key, timestamp, pred_lon.getEntry(0,0),pred_lat.getEntry(0,0),
            MobilityChecker.getSpeedKnots(prediction_result.last, predicted_point2),
            MobilityChecker.getBearing(prediction_result.last, predicted_point2))

          prediction_result += predicted_point2

          var j=1

          while (j<buf_lon.length) {
            buf_lon(j-1)=buf_lon(j)
            buf_lat(j-1)=buf_lat(j)
          }

          buf_lon(buf_lon.length-1)=pred_lon.getEntry(0,0)
          buf_lat(buf_lat.length-1)=pred_lat.getEntry(0,0)

          predictions=predictions+1

        }

        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      }
//todo add error handling
      /*
      var i = 1
      while (i < prediction_result.size) {
        val p = prediction_result(i)

        if (
          p.getSpeed > threshold_broadcastVar.value("MAX_SPEED_THRESHOLD") || p.getSpeed.isNaN || p.getSpeed.isInfinite
        ) {
          p.setError(true)
        } else {
          p.setError(false)
        }

        i += 1
      }
      */
      /*
      if (prediction_result.nonEmpty && prediction_result(1).getError) {
        prediction_error_counter.add(1)
      }
*/
      if (curr_state.size > broadcastVar.value("historical_positions")) {
        state.update(curr_state.slice(curr_state.length-h, curr_state.length))
        //println("mpike edw")
        //println(curr_state.slice(curr_state.length-h, curr_state.length).toList)
      } else {
        state.update(curr_state)
        //println("mpike allou")
      }

      //println(curr_state.toList)
      //System.exit(0)


      prediction_result.iterator
    }

    val predictions = events.groupByKey(events=>events.object_id).flatMapGroupsWithState(
      outputMode = OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(func = currentState)

    predictions.writeStream
      .format("console")
      .outputMode("append")
      .start().awaitTermination()

/*
    predictions
        .writeStream
        .format("parquet")
      .option("path", "predictions_StructuredSessionizationOARIMAGradientDescent")
        .option("checkpointLocation", "checkpoint_structure")
        .outputMode(OutputMode.Append())
        .start.awaitTermination()
  */


  }
}






