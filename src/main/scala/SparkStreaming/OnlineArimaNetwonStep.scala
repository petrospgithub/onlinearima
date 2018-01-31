package SparkStreaming

import java.io.{FileInputStream, InputStream}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

import onlinearima.OARIMA_ons
import org.apache.commons.math3.linear.{AbstractRealMatrix, Array2DRowRealMatrix, MatrixUtils, RealMatrix}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import points.AvroPoint
import utils.{Copy, Interpolation, MobilityChecker, SparkSessionSingleton}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONArray

object OnlineArimaNetwonStep {
  def main(args: Array[String]): Unit = {
    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/onlinearima_configuration.properties")
    val prop: Properties = new Properties()
    prop.load(input)

    val batchInterval = prop.getProperty("batchInterval")

    val input_var = Map(
      "predicted_locations" -> prop.getProperty("Horizon").toInt,
      "historical_positions" -> prop.getProperty("window").toInt,
      "sampling" -> prop.getProperty("sampling").toInt
    )

    val threshold_var = Map(
      "GAP_PERIOD" -> prop.getProperty("GAP_PERIOD").toDouble,
      "MAX_SPEED_THRESHOLD" -> prop.getProperty("MAX_SPEED_THRESHOLD").toDouble
    )


    val sparkConf = new SparkConf().setAppName("OARIMA").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))
    ssc.checkpoint(".checkpoint")

    val broadcastVar = ssc.sparkContext.broadcast(input_var)
    val threshold_broadcastVar = ssc.sparkContext.broadcast(threshold_var)

    val temp=prop.getProperty("epsilon").split("^")

    val lrateVar=ssc.sparkContext.broadcast(prop.getProperty("l_rate").toDouble)
    val epsilonVar=ssc.sparkContext.broadcast(Math.pow(temp.head.toDouble, temp.last.toDouble))

    val prediction_counter = ssc.sparkContext.longAccumulator("prediction_counter")

    val prediction_error_counter = ssc.sparkContext.longAccumulator("prediction_error_counter")

    val lines = ssc.receiverStream(new CustomReceiver(System.getProperty("user.dir") +"/data/"+ args(0)))
    val pointDstream = lines.map(record => {

      val point: Array[String] = record.split(";")
      val p: AvroPoint = new AvroPoint()

      val timeDateStr = point(1)
      val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      // parse to LocalDateTime
      val dt = LocalDateTime.parse(timeDateStr, dtf)

      // assume the LocalDateTime is in UTC
      val instant:Instant = dt.toInstant(ZoneOffset.UTC)

      p.setId(point(0).toInt)
      p.setTimestamp(instant.toEpochMilli)
      p.setLongitude(point(2).toDouble)
      p.setLatitude(point(3).toDouble)
      p.setSpeed(point(5).toDouble)
      p.setHeading(point(4).toDouble)

      (p.getId, p)

    })

    val mappingFunc = (key: Integer, input: Option[AvroPoint], state: State[OARIMAstate_ons]) => {

      val new_point: AvroPoint = input.get
      val h: Int = broadcastVar.value("historical_positions")

      val curr_state: OARIMAstate_ons = if (state.exists()) {

        val points_state = state.get()

        if (new_point.getTimestamp - points_state.state.last.getTimestamp > threshold_broadcastVar.value("GAP_PERIOD")) { //TODO gap threshold

          val w_lon=new Array[Double](h)
          val w_lat=new Array[Double](h)
          val eye=new Array[Double](h)

          var j=0

          while (j<h) {
            w_lon(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0
            w_lat(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0
            eye(j)=epsilonVar.value
            j=j+1
          }

          OARIMAstate_ons(ArrayBuffer(new_point), new Array2DRowRealMatrix, new Array2DRowRealMatrix, MatrixUtils.createRealDiagonalMatrix(eye), MatrixUtils.createRealDiagonalMatrix(eye), w_lon, w_lat, 1, new AvroPoint, new_point)

        } else {
          state.get().state.filterNot(_.getTimestamp == new_point.getTimestamp) += new_point
          val oarima:OARIMAstate_ons=state.get()

          //todo edw na allazw to w!!!
          val w_lon=OARIMA_ons.adapt_w(oarima.prediction.getLongitude, new_point.getLongitude,oarima.w_lon,lrateVar.value, oarima.A_trans_lon, oarima.lon, oarima.i)
          val w_lat=OARIMA_ons.adapt_w(oarima.prediction.getLatitude, new_point.getLatitude,oarima.w_lat,lrateVar.value, oarima.A_trans_lat, oarima.lat, oarima.i)


          OARIMAstate_ons(ArrayBuffer(new_point),new Array2DRowRealMatrix, new Array2DRowRealMatrix, w_lon._2, w_lat._2, w_lon._1, w_lat._1, oarima.i+1, oarima.prediction, new_point)
        }

      } else {
        val w_lon=new Array[Double](h)
        val w_lat=new Array[Double](h)
        val eye=new Array[Double](h)
        var j=0

        while (j<h) {
          w_lon(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0
          w_lat(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0
          eye(j)=epsilonVar.value
          j=j+1
        }

        OARIMAstate_ons(ArrayBuffer(new_point), new Array2DRowRealMatrix, new Array2DRowRealMatrix, MatrixUtils.createRealDiagonalMatrix(eye), MatrixUtils.createRealDiagonalMatrix(eye), w_lon, w_lat, 1, new AvroPoint, new_point)
      }

      val prediction_result: ArrayBuffer[AvroPoint] = new ArrayBuffer[AvroPoint]()

      val sampling = broadcastVar.value("sampling")
      var mode = false

      //var i = 0
      var j = 0
      while (j < curr_state.state.size - 1 && !mode) {
        val elapsedTime = curr_state.state(j + 1).getTimestamp - curr_state.state(j).getTimestamp
        if (elapsedTime != sampling) mode = true
        j += 1
      }

      val spline: Array[AvroPoint] = if (mode) {
        if (Math.floor((curr_state.state.last.getTimestamp - curr_state.state.head.getTimestamp) / sampling.toDouble).toInt + 1 > h && curr_state.state.length > 3) {
          Interpolation.splinepolation2D(curr_state.state.sortWith(_.getTimestamp < _.getTimestamp).toArray, sampling)
        } else {
          Array.empty
        }
      } else {
        Copy.deepCopy(curr_state.state.sortWith(_.getTimestamp < _.getTimestamp).toArray).toArray
      }
      //todo apo edw kai katw dior8wnw


      val spline_length = spline.length
      //var ret = ""
      if (!spline.isEmpty && spline.length >= h) {

        prediction_counter.add(1)
        val trainSet = spline.slice(spline_length - h, spline_length)

        var j=0
        val lon_arr=new Array[Double](trainSet.length)

        val lat_arr=new Array[Double](trainSet.length)
        while (j<trainSet.length) {
          lon_arr(j)=trainSet(j).getLongitude
          lat_arr(j)=trainSet(j).getLatitude
        }

        prediction_result += trainSet.last
        var timestamp = trainSet.last.getTimestamp
        timestamp += sampling
        val predicted_point=new AvroPoint()
        val ret_ogd_lon=OARIMA_ons.prediction(lon_arr, curr_state.w_lon)
        val ret_ogd_lat=OARIMA_ons.prediction(lat_arr, curr_state.w_lat)

        predicted_point.setId(key)
        timestamp += sampling
        predicted_point.setTimestamp(timestamp)
        predicted_point.setLatitude(ret_ogd_lat._1)
        predicted_point.setLongitude(ret_ogd_lon._1)

        predicted_point.setSpeed(MobilityChecker.getSpeedKnots(prediction_result.last, predicted_point))
        predicted_point.setHeading(MobilityChecker.getBearing(prediction_result.last, predicted_point))
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

          val predicted_point2=new AvroPoint()

          predicted_point2.setId(key)
          timestamp += sampling
          predicted_point2.setTimestamp(timestamp)
          predicted_point2.setLatitude(pred_lat.getEntry(0,0))
          predicted_point2.setLongitude(pred_lon.getEntry(0,0))

          predicted_point2.setSpeed(MobilityChecker.getSpeedKnots(prediction_result.last, predicted_point2))
          predicted_point2.setHeading(MobilityChecker.getBearing(prediction_result.last, predicted_point2))
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
      if (prediction_result.nonEmpty && prediction_result(1).getError) {
        prediction_error_counter.add(1)
      }

      if (curr_state.state.size > broadcastVar.value("historical_positions")) curr_state.state.remove(0)

      //todo update state!!!
      //println(curr_state)
      state.update(curr_state)
      println(JSONArray(prediction_result.toList).toString())

      JSONArray(prediction_result.toList).toString()
    }

    val stateDstream = pointDstream.mapWithState(
      StateSpec.function(mappingFunc))

    stateDstream.foreachRDD { (rdd: RDD[String]) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val temp = rdd.toDF()

      temp.write.mode(SaveMode.Append).parquet("predictions_parquet_OARIMA_NetwonStep_output_historical_positions" + prop.getProperty("window") + "_predicted_locations" + prop.getProperty("Horizon")+"_"+args(1).split("/").last )
    }


    ssc.start()
    ssc.awaitTermination()

    println(prediction_counter)
    println(prediction_error_counter)
  }
}
