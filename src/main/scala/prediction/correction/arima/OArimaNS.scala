package prediction.correction.arima

import onlinearima.OARIMA_ons
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import simulation.CustomReceiver
import types.{OArimastateNS, STPoint}
import utils.{Copy, Interpolation, MobilityChecker, SparkSessionSingleton}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object OArimaNS {
  def main(args: Array[String]): Unit = {

    val batchInterval=1
    val sparkConf = new SparkConf().setAppName("OArimaNS")

    /* Spark Init */
    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))
    ssc.checkpoint(".checkpoint")

    val prop=ssc.sparkContext.getConf

    /* Model Configuration */
    val window=prop.get("spark.window").toInt
    val train_set=prop.get("spark.train_set").toInt
    val horizon=prop.get("spark.horizon").toInt
    val lrate=prop.get("spark.lrate").toDouble
    val sampling=prop.get("spark.sampling").toInt
    val epsilon=prop.get("spark.epsilon").toDouble
    val gap_threshold=prop.get("spark.gap_threshold").toInt
    val speed_threshold=prop.get("spark.speed_threshold").toDouble
    val path=prop.get("spark.path")
    val mode = prop.get("spark.mode")
    val topics = prop.get("spark.topics")
    val brokers = prop.get("spark.brokers")
    val groupId = prop.get("spark.groupid")
    val vectorinit = prop.get("spark.vectorinit")

    if (train_set>=window) {
      println("Window parameter must be greater than train_set")
      System.exit(1)
    }

    val broadcastTrain=ssc.sparkContext.broadcast(train_set)
    val broadcastGAP=ssc.sparkContext.broadcast(gap_threshold)
    val broadcastHistory=ssc.sparkContext.broadcast(window)
    val broadcastSampling=ssc.sparkContext.broadcast(sampling)
    val broadcastHorizon=ssc.sparkContext.broadcast(horizon)
    val broadcastLRATE=ssc.sparkContext.broadcast(lrate)
    val broadcastSpeedThres=ssc.sparkContext.broadcast(speed_threshold)
    val broadcastEpsilon=ssc.sparkContext.broadcast(epsilon)
    val broadcastpath=ssc.sparkContext.broadcast(path)
    val broadcastVector=ssc.sparkContext.broadcast(vectorinit)

    /* Create (K=id,V=spatiotemporal point) for stateful streaming processing */

    val pointDstream = mode match {
      case "simulation" => ssc.receiverStream(new CustomReceiver(System.getProperty("user.dir") + "/data/" + path))
        .map(record => {
          val point: Array[String] = record.split(",")

          if (point(1).toLong==1452557976000L) {
            System.exit(0)
          }

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

    val mappingFunc = (key: Int, input: Option[STPoint], state: State[OArimastateNS]) => {

      val new_point: STPoint = input.get
      val h=broadcastHistory.value
      val Horizon=broadcastHorizon.value
      val wLen=broadcastTrain.value
      val epsilon=broadcastEpsilon.value
      /* Update Moving Object State */

      val state_new:OArimastateNS = if (state.exists()) {

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

          val temp_state=OArimastateNS(Some(Array(new_point)),None,None,None,None,None,None,None,None)

          //Array(new_point)

          temp_state.w_lon=Some(w_lon)
          temp_state.w_lat=Some(w_lat)
          temp_state.w_speed=Some(w_speed)
          temp_state.w_heading=Some(w_heading)

          val eye=new Array[Double](wLen)
          var j=0

          while (j<wLen) {
            eye(j)=epsilon
            j=j+1
          }

          temp_state.A_TransLON=Some(MatrixUtils.createRealDiagonalMatrix(eye))
          temp_state.A_TransLAT=Some(MatrixUtils.createRealDiagonalMatrix(eye))

          temp_state.A_TransSPEED=Some(MatrixUtils.createRealDiagonalMatrix(eye))
          temp_state.A_TransHEADING=Some(MatrixUtils.createRealDiagonalMatrix(eye))


          temp_state
        } else {

          val temp_state:OArimastateNS=state.get()
          val arr=temp_state.history

          temp_state.history=Some(arr.get.padTo(arr.get.length+1, new_point))

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

        val temp_state=OArimastateNS(Some(Array(new_point)),None,None,None,None,None,None,None,None)

        temp_state.w_lon=Some(w_lon)
        temp_state.w_lat=Some(w_lat)
        temp_state.w_speed=Some(w_speed)
        temp_state.w_heading=Some(w_heading)


        val eye=new Array[Double](wLen)
        var j=0

        while (j<wLen) {
          eye(j)=epsilon
          j=j+1
        }

        temp_state.A_TransLON=Some(MatrixUtils.createRealDiagonalMatrix(eye))
        temp_state.A_TransLAT=Some(MatrixUtils.createRealDiagonalMatrix(eye))

        temp_state.A_TransSPEED=Some(MatrixUtils.createRealDiagonalMatrix(eye))
        temp_state.A_TransHEADING=Some(MatrixUtils.createRealDiagonalMatrix(eye))

        temp_state
      }

      val prediction_result: Array[STPoint] = new Array[STPoint](Horizon+1)

      /* Fix Sampling */

      val sampling = broadcastSampling.value
      var mode = false

      //var i = 0
      var j = 0
      while (j < state_new.history.get.length - 1 && !mode) {
        val elapsedTime = state_new.history.get.apply(j+1).timestamp - state_new.history.get.apply(j).timestamp
        if (elapsedTime != sampling) mode = true
        j += 1
      }

      val spline: Array[STPoint] = if (mode) {
        if (Math.floor((state_new.history.get.last.timestamp - state_new.history.get.head.timestamp) / sampling.toDouble).toInt + 1 > h) {
          Interpolation.splinepolation2D(state_new.history.get.sortWith(_.timestamp < _.timestamp), sampling)
        } else {
          Array.empty
        }
      } else {
        Copy.deepCopy(state_new.history.get.sortWith(_.timestamp < _.timestamp))
      }

      if (!spline.isEmpty && spline.length >= h) {
        var splitAt=broadcastTrain.value
        var start=0

        /* Train Arima Model */

        val v_spline:Array[STPoint] = if (spline.length==h) {

          Array(STPoint(key, spline.head.timestamp,
            0,
            0,
            spline.head.speed, spline.head.heading, spline.head.error
          )) ++ spline.sliding(2).map(f=>{
            STPoint(f.head.id, f.last.timestamp,
              f.last.longitude-f.head.longitude,
              f.last.latitude-f.head.latitude,
              f.last.speed-f.head.speed, f.last.heading-f.head.heading, f.last.error
            )
          })

        } else {
          spline.slice(spline.length - h-1, spline.length).sliding(2).map(f=>{

            STPoint(f.head.id, f.last.timestamp,
              f.last.longitude-f.head.longitude,
              f.last.latitude-f.head.latitude,
              f.last.speed-f.head.speed, f.last.heading-f.head.heading, f.last.error
            )
          }).toArray
        }

        val correctionLon=new ArrayBuffer[Double]()
        val correctionLat=new ArrayBuffer[Double]()
        val correctionSpeed=new ArrayBuffer[Double]()
        val correctionHeading=new ArrayBuffer[Double]()

        while (splitAt < h) {

          val train=v_spline.slice(start,splitAt)
          val test=v_spline(splitAt)
          //val train=split_tuple._1
          //val test=split_tuple._2.head

          /* Training */
          val data_lon=train.map(x=>x.longitude)
          val data_lat=train.map(x=>x.latitude)

          val data_speed=train.map(x=>x.speed)
          val data_heading=train.map(x=>x.heading)

          val prediction_lon=OARIMA_ons.prediction(data_lon, state_new.w_lon.get) //TODO
          val prediction_lat=OARIMA_ons.prediction(data_lat,  state_new.w_lat.get) //TODO

          val prediction_speed=OARIMA_ons.prediction(data_speed, state_new.w_speed.get) //TODO
          val prediction_heading=OARIMA_ons.prediction(data_heading,  state_new.w_heading.get) //TODO
          //point, diff, adapt!!!

          //val diff=Distance.getHaversineDistance(prediction_lat,prediction_lon, test.latitude, test.longitude)

          val new_wALon=OARIMA_ons.adapt_w_diff(
            prediction_lon,
            test.longitude,
            state_new.w_lon.get, broadcastLRATE.value,
            data_lon,
            state_new.A_TransLON.get
          )

          val new_wALat=OARIMA_ons.adapt_w_diff(
            prediction_lat,
            test.latitude,
            state_new.w_lat.get, broadcastLRATE.value,
            data_lat,
            state_new.A_TransLAT.get
          )


          val new_wAspeed=OARIMA_ons.adapt_w_diff(
            prediction_speed,
            test.speed,
            state_new.w_speed.get, broadcastLRATE.value,
            data_speed,
            state_new.A_TransSPEED.get
          )

          val new_wAheading=OARIMA_ons.adapt_w_diff(
            prediction_heading,
            test.heading,
            state_new.w_heading.get, broadcastLRATE.value,
            data_heading,
            state_new.A_TransHEADING.get
          )

          state_new.w_lon=Some(new_wALon._1)
          state_new.A_TransLON=Some(new_wALon._2)

          state_new.w_lat=Some(new_wALat._1)
          state_new.A_TransLAT=Some(new_wALat._2)


          state_new.w_speed=Some(new_wAspeed._1)
          state_new.A_TransSPEED=Some(new_wAspeed._2)

          state_new.w_heading=Some(new_wAheading._1)
          state_new.A_TransHEADING=Some(new_wAheading._2)

          correctionLon.append(new_wALon._3)
          correctionLat.append(new_wALat._3)
          correctionSpeed.append(new_wAspeed._3)
          correctionHeading.append(new_wAheading._3)

          start=start+1
          splitAt=splitAt+1
        }

        prediction_result(0) = state_new.history.get.last

        var predictions=1

        val data=spline.slice(spline.length-wLen, spline.length)

        val data_lon=data.map(x=>x.longitude)
        val data_lat=data.map(x=>x.latitude)


        val data_speed=data.map(x=>x.speed)
        val data_heading=data.map(x=>x.heading)

        val lastT=v_spline.last.timestamp

        /* Prediction */

        val lonMeandiff=correctionLon.sum/correctionLon.length.toDouble
        val latMeandiff=correctionLat.sum/correctionLat.length.toDouble
        val speedMeandiff=correctionSpeed.sum/correctionSpeed.length.toDouble
        val headingMeandiff=correctionHeading.sum/correctionHeading.length.toDouble

        while (predictions<=Horizon) {

          val point = STPoint(
            key,
            lastT + (predictions * sampling),
            prediction_result(predictions - 1).longitude+OARIMA_ons.prediction(data_lon, state_new.w_lon.get, lonMeandiff),
            prediction_result(predictions - 1).latitude+OARIMA_ons.prediction(data_lat, state_new.w_lat.get, latMeandiff),
            prediction_result(predictions - 1).speed+OARIMA_ons.prediction(data_speed, state_new.w_speed.get, speedMeandiff),
            prediction_result(predictions - 1).heading+OARIMA_ons.prediction(data_heading, state_new.w_heading.get, headingMeandiff),
             error = false
          )


      //    val speed=MobilityChecker.getSpeedKnots(prediction_result(predictions-1), point)
       //   val heading=MobilityChecker.getBearing(prediction_result(predictions-1), point)

       //   point.speed=speed
        //  point.heading=heading

          /*Error Checker*/

          if (
            point.speed > broadcastSpeedThres.value || point.speed.isNaN || point.speed.isInfinite /*|| p.getAltitude > 40000*/
          ) {
            point.error=true
          } else {
            point.error=false
          }

          prediction_result(predictions)=point

          predictions=predictions+1
        }

      }

      /* Update State */

      if (state_new.history.get.length > h) {
        val new_arr=state_new.history.get.slice(state_new.history.get.length-h, state_new.history.get.length)
        state_new.history=Some(new_arr)
        state.update(state_new)
      } else {
        state.update(state_new)
      }

      prediction_result
    }

    val stateDstream = pointDstream.mapWithState(
      StateSpec.function(mappingFunc))


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
          var j = 0
          if (x.head != null) {
            while (j < x.length) {
              val p = x(j)
              foo.append(Row(p.id, p.timestamp, p.longitude, p.latitude, p.speed, p.heading, p.error, j))
              j = j + 1
            }
          }
          foo.iterator
        }), schema).write.mode(SaveMode.Append).parquet("correction_predictions_parquet_OArimaNSoutput_historical_positions" + broadcastHistory.value + "_predicted_locations" + broadcastHorizon.value + "_sampling_"+broadcastSampling.value+"_lrate_"+broadcastLRATE.value.toString.replace(".", "")+"_train_"+broadcastTrain.value+"_"+broadcastpath.value.replace(".csv",""))
      }
    }


    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

