package prediction.arma

import java.util

import onlinearima.OARIMA_ogd
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import simulation.CustomReceiver
import types.{OArimastateGD, STPoint}
import utils.{Copy, Interpolation, MobilityChecker, SparkSessionSingleton}

import scala.collection.mutable.ListBuffer

object OARMAGD {

  def main(args: Array[String]): Unit = {

    val batchInterval = 1
    val sparkConf = new SparkConf().setAppName("OARMAGD")

    /* Spark Init */
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))
    ssc.checkpoint(".checkpoint")

    ssc.sparkContext.setLogLevel("WARN")

    val prop = ssc.sparkContext.getConf

    /* Model Configuration */

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
    val vectorinit = prop.get("spark.vectorinit")

    if (train_set >= window) {
      println("Window parameter must be greater than train_set")
      System.exit(1)
    }

    val broadcastTrain = ssc.sparkContext.broadcast(train_set)
    val broadcastGAP = ssc.sparkContext.broadcast(gap_threshold)
    val broadcastHistory = ssc.sparkContext.broadcast(window)
    val broadcastSampling = ssc.sparkContext.broadcast(sampling)
    val broadcastHorizon = ssc.sparkContext.broadcast(horizon)
    val broadcastLRATE = ssc.sparkContext.broadcast(lrate)
    val broadcastSpeedThres = ssc.sparkContext.broadcast(speed_threshold)
    val broadcastVector=ssc.sparkContext.broadcast(vectorinit)

    val broadcastpath=ssc.sparkContext.broadcast(path)

    /* Create (K=id,V=spatiotemporal point) for stateful streaming processing */

    val pointDstream = mode match {
      case "simulation" => ssc.receiverStream(new CustomReceiver(System.getProperty("user.dir") + "/data/" + path))
        .map(record => {
          val point: Array[String] = record.split(",")

          (point(0).toInt, STPoint(
            point(0).toInt, point(1).toLong, point(2).toDouble, point(3).toDouble,
            point(4).toDouble, point(5).toDouble, error = false)
          )
        })
      case "kafka" =>
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
          ConsumerConfig.GROUP_ID_CONFIG -> groupId,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

        val messages = KafkaUtils.createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

        messages.map(record => {
          val point: Array[String] = record.value().split(",")
          (point(0).toInt, STPoint(
            point(0).toInt, point(1).toLong, point(2).toDouble, point(3).toDouble,
            point(4).toDouble, point(5).toDouble, error = false)
          )
        })
    }

    /* Stateful Streaming Processing */
    val mappingFunc = (key: Int, input: Option[STPoint], state: State[OArimastateGD]) => {

      val new_point: STPoint = input.get
      val h = broadcastHistory.value
      val Horizon = broadcastHorizon.value
      val wLen = broadcastTrain.value

      /* Update Moving Object State */

      val state_new: OArimastateGD = if (state.exists()) {

        if (state.isTimingOut() || new_point.timestamp - state.get().history.get.last.timestamp > broadcastGAP.value) {

          val w_lon:Array[Double]=broadcastVector.value match {
            case "linear" =>
              val linear = Array[Double](2.0, -1.0)
              val w_lon = linear.padTo(wLen, 0.0)
              w_lon
            case "random" =>
              val start = -100
              val end   = 100
              val rnd = new scala.util.Random

              Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
          }

          val w_lat:Array[Double]=broadcastVector.value match {
            case "linear" =>
              val linear = Array[Double](2.0, -1.0)
              val w_lat = linear.padTo(wLen, 0.0)
              w_lat
            case "random" =>
              val start = -100
              val end   = 100
              val rnd = new scala.util.Random

              Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
          }

          val w_speed:Array[Double]=broadcastVector.value match {
            case "linear" =>
              val linear = Array[Double](2.0, -1.0)
              val w_lat = linear.padTo(wLen, 0.0)
              w_lat
            case "random" =>
              val start = -100
              val end   = 100
              val rnd = new scala.util.Random

              Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
          }

          val w_heading:Array[Double]=broadcastVector.value match {
            case "linear" =>
              val linear = Array[Double](2.0, -1.0)
              val w_lat = linear.padTo(wLen, 0.0)
              w_lat
            case "random" =>
              val start = -100
              val end   = 100
              val rnd = new scala.util.Random

              Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
          }


          //  val temp_state:OArimastateGD = OArimastateGD(Array(new_point))

          val temp_state=OArimastateGD(Some(Array(new_point)),None,None,None,None,None)

          temp_state.w_lon=Some(w_lon)
          temp_state.w_lat=Some(w_lat)
          temp_state.w_speed=Some(w_speed)
          temp_state.w_heading=Some(w_heading)

          temp_state.i=Some(1)

          temp_state
        } else {

          val temp_state: OArimastateGD = state.get()
          val arr = temp_state.history
          temp_state.history = Some(arr.get.padTo(arr.get.length + 1, new_point))
          temp_state.i=Some(temp_state.i.get + 1)

          temp_state
        }

      } else {

        val w_lon:Array[Double]=broadcastVector.value match {
          case "linear" =>
            val linear = Array[Double](2.0, -1.0)
            val w_lon = linear.padTo(wLen, 0.0)
            w_lon
          case "random" =>
            val start = -100
            val end   = 100
            val rnd = new scala.util.Random

            Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
        }

        val w_lat:Array[Double]=broadcastVector.value match {
          case "linear" =>
            val linear = Array[Double](2.0, -1.0)
            val w_lat = linear.padTo(wLen, 0.0)
            w_lat
          case "random" =>
            val start = -100
            val end   = 100
            val rnd = new scala.util.Random

            Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
        }

        val w_speed:Array[Double]=broadcastVector.value match {
          case "linear" =>
            val linear = Array[Double](2.0, -1.0)
            val w_lat = linear.padTo(wLen, 0.0)
            w_lat
          case "random" =>
            val start = -100
            val end   = 100
            val rnd = new scala.util.Random

            Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
        }

        val w_heading:Array[Double]=broadcastVector.value match {
          case "linear" =>
            val linear = Array[Double](2.0, -1.0)
            val w_lat = linear.padTo(wLen, 0.0)
            w_lat
          case "random" =>
            val start = -100
            val end   = 100
            val rnd = new scala.util.Random

            Array.fill(wLen){(start + rnd.nextInt( (end - start) + 1 ))/100.0 }
        }


        val temp_state=OArimastateGD(Some(Array(new_point)),None,None,None,None,None)

        temp_state.w_lon=Some(w_lon)
        temp_state.w_lat=Some(w_lat)
        temp_state.w_speed=Some(w_speed)
        temp_state.w_heading=Some(w_heading)

        temp_state.i=Some(1)

        temp_state
      }

      val prediction_result: Array[STPoint] = new Array[STPoint](Horizon + wLen)

      /* Fix Sampling */
      val sampling = broadcastSampling.value
      var mode = false

      var j = 0
      while (j < state_new.history.get.length - 1 && !mode) {
        val elapsedTime = state_new.history.get.apply(j + 1).timestamp - state_new.history.get.apply(j).timestamp
        if (elapsedTime != sampling) mode = true
        j += 1
      }

      val spline: Array[STPoint] = if (mode) {
        if (Math.floor((state_new.history.get.last.timestamp - state_new.history.get.head.timestamp) / sampling.toDouble).toInt + 1 > h) {
          val interp = Interpolation.splinepolation2D(state_new.history.get.sortWith(_.timestamp < _.timestamp), sampling)
          state_new.i=Some(interp.length+state_new.i.get)
          interp
        } else {
          Array.empty
        }
      } else {
        Copy.deepCopy(state_new.history.get.sortWith(_.timestamp < _.timestamp))
      }

   //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

     //println("state_new.w_lon: "+util.Arrays.toString(state_new.w_lon.get))
    // println("state_new.w_lat: "+util.Arrays.toString(state_new.w_lat.get))
    // println("spline: "+ util.Arrays.toString(spline.asInstanceOf[Array[AnyRef]]))

      if (!spline.isEmpty && spline.length >= h) {

        var splitAt = broadcastTrain.value
        var start = 0

        val v_spline = spline.slice(spline.length - h, spline.length)//util.Arrays.copyOfRange(spline, spline.length - h, spline.length)//

        /* Train Arima Model */

        while (splitAt < h) {
          val train = v_spline.slice(start, splitAt)//util.Arrays.copyOfRange(v_spline, start, splitAt)//
          val test = v_spline(splitAt)

          val data_lon = train.map(x => x.longitude)
          val data_lat = train.map(x => x.latitude)

          val data_speed=train.map(x=>x.speed)
          val data_heading=train.map(x=>x.heading)

          val prediction_lon=OARIMA_ogd.prediction(data_lon, state_new.w_lon.get) //TODO
          val prediction_lat=OARIMA_ogd.prediction(data_lat,  state_new.w_lat.get) //TODO

         val prediction_speed=OARIMA_ogd.prediction(data_speed, state_new.w_speed.get) //TODO
         val prediction_heading=OARIMA_ogd.prediction(data_heading,  state_new.w_heading.get) //TODO
/*
          println("splitAt: "+splitAt)

          println("data lon: "+util.Arrays.toString(data_lon))
          println("data lat: "+util.Arrays.toString(data_lat))

          println("train: "+util.Arrays.toString(train.asInstanceOf[Array[AnyRef]]))

          println("real point: POINT("+test.longitude+" "+test.latitude+")")

          println("prediction point: POINT("+prediction_lon+" "+prediction_lat+")")

          println("havershine: "+MobilityChecker.getHaversineDistance(test.latitude, test.longitude, prediction_lat, prediction_lon))
          println("euclidean: "+MobilityChecker.getEuclidean(test.latitude, test.longitude, prediction_lat, prediction_lon))
*/
          val new_wLon = OARIMA_ogd.adapt_w(
            prediction_lon,
            test.longitude,
            state_new.w_lon.get, broadcastLRATE.value,
            data_lon, state_new.i.get
          )
          val new_wLat = OARIMA_ogd.adapt_w(
            prediction_lat,
            test.latitude,
            state_new.w_lat.get, broadcastLRATE.value,
            data_lat, state_new.i.get
          )

          val new_wSpeed = OARIMA_ogd.adapt_w(
            prediction_speed,
            test.speed,
            state_new.w_speed.get, broadcastLRATE.value,
            data_speed, state_new.i.get
          )
          val new_wHeading = OARIMA_ogd.adapt_w(
            prediction_heading,
            test.heading,
            state_new.w_heading.get, broadcastLRATE.value,
            data_heading, state_new.i.get
          )

/*
          println("new_wLon: "+util.Arrays.toString(new_wLon))
          println("new_wLat: "+util.Arrays.toString(new_wLat))

          println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
*/
          state_new.w_lon=Some(new_wLon)
          state_new.w_lat=Some(new_wLat)
          state_new.w_speed=Some(new_wSpeed)
          state_new.w_heading=Some(new_wHeading)

          start = start + 1
          splitAt = splitAt + 1
        }

        //prediction_result(0) = state_new.history.get.last

        var predictions = 0
        val lastT = v_spline.last.timestamp
        var i=0
        while (i<wLen) {
          prediction_result(i)=v_spline(v_spline.length-wLen+i)
          i=i+1
        }

        /* Prediction */
        while (predictions < Horizon) {

          val data=prediction_result.slice(predictions,predictions+wLen)//util.Arrays.copyOfRange(prediction_result, predictions, predictions+wLen)

          val data_lon = data.map(x => x.longitude)
          val data_lat = data.map(x => x.latitude)
          val data_speed=data.map(x=>x.speed)
          val data_heading=data.map(x=>x.heading)

          val point = STPoint(
            key,
            lastT + ((predictions+1) * sampling),
            OARIMA_ogd.prediction(data_lon, state_new.w_lon.get),
            OARIMA_ogd.prediction(data_lat, state_new.w_lat.get),

            OARIMA_ogd.prediction(data_speed, state_new.w_speed.get),
            OARIMA_ogd.prediction(data_heading, state_new.w_heading.get),
             error = false
          )


       //   val speed = MobilityChecker.getSpeedKnots(prediction_result(predictions - 1), point)
        //  val heading = MobilityChecker.getBearing(prediction_result(predictions - 1), point)

         // point.speed = speed
         // point.heading = heading

          /*Error Checker*/

          if (
            point.speed > broadcastSpeedThres.value || point.speed.isNaN || point.speed.isInfinite /*|| p.getAltitude > 40000*/
          ) {
            point.error = true
          } else {
            point.error = false
          }

          prediction_result(predictions+wLen) = point

          predictions = predictions + 1
        }

      }

      val final_pred=prediction_result.slice(prediction_result.length-Horizon,prediction_result.length)

    // println("prediction_result.toList: "+util.Arrays.toString(final_pred.asInstanceOf[Array[AnyRef]]))

   // println("------------------------------------------------------------------")


      /* Update State */
      if (state_new.history.get.length > h) {
        val new_arr = state_new.history.get.slice(state_new.history.get.length - h, state_new.history.get.length)
        state_new.history = Some(new_arr)
        state.update(state_new)
      } else {
        state.update(state_new)
      }

      final_pred
    }

    val stateDstream = pointDstream.mapWithState(
      StateSpec.function(mappingFunc).timeout(Milliseconds(gap_threshold)))

    /* Store for evaluation*/
    stateDstream.foreachRDD { rdd: RDD[Array[STPoint]] =>

      if (!rdd.isEmpty()) {

        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

        val schema = StructType(
          Array(
            StructField("id", IntegerType),
            StructField("timestamp", LongType),
            StructField("longitude", DoubleType),
            StructField("latitude", DoubleType),
            //StructField("altitude", DoubleType),
            StructField("speed", DoubleType),
            StructField("heading", DoubleType),
            StructField("error", BooleanType),
            StructField("horizon", IntegerType)
          )
        )

        spark.createDataFrame(rdd.flatMap(x => {

          val foo = new ListBuffer[Row]()
          var j = 1
          if (x.head != null) {

            if (x.head.timestamp==1452557977000L) {
              System.exit(0)
            }

            while (j < x.length) {
              val p = x(j)
              foo.append(Row(p.id, p.timestamp, p.longitude, p.latitude, p.speed, p.heading, p.error, j))
              j = j + 1
            }
          }
          foo.iterator
        }), schema).write.mode(SaveMode.Append).parquet("predictions_parquet_OARMAGDoutput_historical_positions" + broadcastHistory.value + "_predicted_locations" + broadcastHorizon.value + "_sampling_" + broadcastSampling.value + "_lrate_" + broadcastLRATE.value.toString.replace(".", "") + "_train_" + broadcastTrain.value+"_"+broadcastpath.value.replace(".csv",""))
      }
    }


    //stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
