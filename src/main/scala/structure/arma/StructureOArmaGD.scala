package structure.arma
/*
import onlinearima.OARIMA_ogd
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import types.{OArimastateGD, STPoint}
import utils.{Copy, Interpolation, MobilityChecker}

object StructureOArmaGD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredOArmaGD")
      .getOrCreate()

    val prop = spark.sparkContext.getConf


    val window = prop.get("spark.window").toInt
    val train_set = prop.get("spark.train_set").toInt
    val horizon = prop.get("spark.horizon").toInt
    val lrate = prop.get("spark.lrate").toDouble
    val sampling = prop.get("spark.sampling").toInt
    val gap_threshold = prop.get("spark.gap_threshold").toInt
    val speed_threshold = prop.get("spark.speed_threshold").toDouble
    val path = prop.get("spark.path")
    val mode = prop.get("spark.mode")
    val topics = prop.get("spark.topics")
    val brokers = prop.get("spark.brokers")
    val groupId = prop.get("spark.groupid")

    val bootstrapServers= prop.get("spark.bootstrapservers")

    val broadcastTrain = spark.sparkContext.broadcast(train_set)
    val broadcastGAP = spark.sparkContext.broadcast(gap_threshold)
    val broadcastHistory = spark.sparkContext.broadcast(window)
    val broadcastSampling = spark.sparkContext.broadcast(sampling)
    val broadcastHorizon = spark.sparkContext.broadcast(horizon)
    val broadcastLRATE = spark.sparkContext.broadcast(lrate)
    val broadcastSpeedThres = spark.sparkContext.broadcast(speed_threshold)

    import spark.implicits._

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Split the lines into words, treat words as sessionId of events
    val events = lines.map(record => {
      val point: Array[String] = record.split(",")
      STPoint(
        point(0).toInt, point(1).toLong, point(2).toDouble, point(3).toDouble,
        0.0, 0.0, error = false)
    })

    def currentState(key: Int, events: Iterator[STPoint], state: GroupState[OArimastateGD]): Iterator[STPoint] = {

      val h = broadcastHistory.value
      val Horizon = broadcastHorizon.value
      val wLen = broadcastTrain.value

      val state_new:OArimastateGD=if (state.exists) {


        if (state.hasTimedOut) {

          val linear = Array[Double](2.0, -1.0)
          val w_lon = linear.padTo(wLen, 0.0)
          val w_lat = linear.padTo(wLen, 0.0)

          val temp_state:OArimastateGD = OArimastateGD(events.toArray)

          temp_state.setterWLON(w_lon)
          temp_state.setterWLAT(w_lat)

          temp_state.setterI(1)

          temp_state
        } else {

          val temp_state: OArimastateGD = state.get
          val arr = temp_state.history.asInstanceOf[Array[STPoint]]
          temp_state.history = arr.union(events.toArray.asInstanceOf[Array[STPoint]])
          temp_state.setterI(temp_state.getterI() + 1)

          temp_state
        }
      } else {
        val linear = Array[Double](2.0, -1.0)
        val w_lon = linear.padTo(wLen, 0.0)
        val w_lat = linear.padTo(wLen, 0.0)

        val temp_state:OArimastateGD = OArimastateGD(events.toArray)

        temp_state.setterWLON(w_lon)
        temp_state.setterWLAT(w_lat)
        temp_state.setterI(1)

        temp_state
      }

      val prediction_result: Array[STPoint] = new Array[STPoint](Horizon + 1)

      /* Fix Sampling */
      val sampling = broadcastSampling.value
      var mode = false

      var j = 0
      while (j < state_new.history.length - 1 && !mode) {
        val elapsedTime = state_new.history.apply(j + 1).timestamp - state_new.history.apply(j).timestamp
        if (elapsedTime != sampling) mode = true
        j += 1
      }

      val spline: Array[STPoint] = if (mode) {
        if (Math.floor((state_new.history.last.timestamp - state_new.history.head.timestamp) / sampling.toDouble).toInt + 1 > h) {
          val interp = Interpolation.splinepolation2D(state_new.history.sortWith(_.timestamp < _.timestamp), sampling)
          state_new.setterI(interp.length)
          interp
        } else {
          Array.empty
        }
      } else {
        Copy.deepCopy(state_new.history.sortWith(_.timestamp < _.timestamp))
      }

      if (!spline.isEmpty && spline.length >= h) {

        var splitAt = broadcastTrain.value
        var start = 0

        val v_spline = spline.slice(spline.length - h, spline.length)

        /* Train Arima Model */

        while (splitAt < h) {
          val train = v_spline.slice(start, splitAt)
          val test = v_spline(splitAt)

          val data_lon = train.map(x => x.longitude)
          val data_lat = train.map(x => x.latitude)

          val prediction_lon = OARIMA_ogd.prediction(data_lon, state_new.getterWLON())
          val prediction_lat = OARIMA_ogd.prediction(data_lat, state_new.getterWLAT())

          val new_wLon = OARIMA_ogd.adapt_w(
            prediction_lon,
            test.longitude,
            state_new.getterWLON(), broadcastLRATE.value,
            data_lon, state_new.getterI()
          )
          val new_wLat = OARIMA_ogd.adapt_w(
            prediction_lat,
            test.latitude,
            state_new.getterWLAT(), broadcastLRATE.value,
            data_lat, state_new.getterI()
          )

          state_new.setterWLON(new_wLon)
          state_new.setterWLAT(new_wLat)

          start = start + 1
          splitAt = splitAt + 1
        }

        prediction_result(0) = state_new.history.last

        var predictions = 1

        val data = spline.slice(spline.length - wLen, spline.length)

        val data_lon = data.map(x => x.longitude)
        val data_lat = data.map(x => x.latitude)
        val lastT = v_spline.last.timestamp

        /* Prediction */
        while (predictions <= Horizon) {

          val point = STPoint(
            key,
            lastT + (predictions * sampling),
            OARIMA_ogd.prediction(data_lon, state_new.getterWLON()),
            OARIMA_ogd.prediction(data_lat, state_new.getterWLAT()),
            0, 0, error = false
          )


          val speed = MobilityChecker.getSpeedKnots(prediction_result(predictions - 1), point)
          val heading = MobilityChecker.getBearing(prediction_result(predictions - 1), point)

          point.speed = speed
          point.heading = heading

          /*Error Checker*/

          if (
            point.speed > broadcastSpeedThres.value || point.speed.isNaN || point.speed.isInfinite /*|| p.getAltitude > 40000*/
          ) {
            point.error = true
          } else {
            point.error = false
          }

          prediction_result(predictions) = point

          predictions = predictions + 1
        }

      }

      /* Update State */
      if (state_new.history.length > h) {
        val new_arr = state_new.history.slice(state_new.history.length - h, state_new.history.length)
        state_new.history = new_arr
        state.update(state_new)
      } else {
        state.update(state_new)
      }


      prediction_result.toIterator

    }

    val sessionUpdates = events
      .groupByKey(event => event.id).flatMapGroupsWithState(
      outputMode = OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(func = currentState)


    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .format("parquet")
      .option("path", "predictions_StructuredSessionizationOArmaGD")
      .option("checkpointLocation", "checkpoint_structure")
      .outputMode(OutputMode.Append())
      .start.awaitTermination()

  }
}
*/