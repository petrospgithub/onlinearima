package evaluation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import types.STPoint
import utils.MobilityChecker

object Accuracy {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder
      .appName("PredictionEvaluation")
      .getOrCreate()


    val prop=spark.sparkContext.getConf
    //prop.load(input)

    val host = prop.get("host")
    val user = prop.get("user")
    val pass = prop.get("pass")

    val insert=prop.get("query_insert")
    val select=prop.get("query_select")
    val path=prop.get("path")
    val horizon=prop.get("horizon")

    val postgres: Broadcast[PostgresBroadcast] = {
      spark.sparkContext.broadcast(PostgresBroadcast(host,user,pass))
    }

    val selectVar=spark.sparkContext.broadcast(select)
    val insertVar=spark.sparkContext.broadcast(insert)
    val pathVar=spark.sparkContext.broadcast(path)
    val error_counter=spark.sparkContext.longAccumulator("error_counter")

    val df=spark.read.parquet(path)

    df.filter("horizon="+horizon).foreachPartition(partitionOfRecords => {

      partitionOfRecords.foreach(record => {

        val predicted_point= STPoint(
          record.getAs[Int](0),
          record.getAs[Long](1),
          record.getAs[Double](2),
          record.getAs[Double](3),
          record.getAs[Double](4),
          record.getAs[Double](5),
          record.getAs[Boolean](6)
        )


        val Horizon=record.getAs[Int](8)

        if (!predicted_point.error) {
          val rs = postgres.value.select(selectVar.value, predicted_point.id, predicted_point.timestamp)

          while (rs.next()) {

            val point = STPoint(
              rs.getInt(1),
              rs.getLong(2),
              rs.getDouble(3),
              rs.getDouble(4),
              0.0,
              0.0,
              error = false
            )

            val dist = MobilityChecker.getHaversineDistance(predicted_point.latitude, predicted_point.longitude, point.latitude,point.longitude)
            //val alt = Math.abs(predicted_point.getAltitude*0.3048 - point.getAltitude*0.3048)

            postgres.value.insert(insertVar.value, pathVar.value+"_Horizon_"+Horizon, predicted_point.timestamp, Horizon, dist, 0)

          }

        } else {
          error_counter.add(1)
        }
      })
    })

    //println("Lon-Lat: "+Math.sqrt(sum_accum_2d.avg))
    //println("Alt: "+Math.sqrt(sum_accum_alt.avg))
    println("Error Counter: "+error_counter)

    spark.close()

  }

}
