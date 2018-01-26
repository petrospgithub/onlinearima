package sql.streaming

import org.apache.spark.sql.SparkSession

/*
 * Example:
 *    `$ bin/run-example \
 *      sql.streaming.StructuredKafkaWordCount host1:port1,host2:port2 \
 *      subscribe topic1,topic2`
 */


object SparkStructuredStreamingWordCount {
  def main(args:Array[String]): Unit = {
    /*
    if (args.length < 3) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>")
      System.exit(1)
    }
    */

    val Array(bootstrapServers, subscribeType, topics) = args

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
/*
    val geom2Text = udf { input: String =>
      try {
        /*
        val geom = ReadPostgresGeom.hex2Geom(input)
        geom.toText
        */
      } catch {
        case e: NullPointerException => null
      }
    }

    val rdd = wordCounts.withColumn("geom2Text", geom2Text('geom)).rdd
      .map(row => {
        val index = row.fieldIndex("geom2Text")
        (STObject.apply(row.get(index).toString), row)
      })

    val minMax = SpatialPartitioner.getMinMax(rdd)

    val partitioner = args(0) match {
      case "grid" =>new SpatialGridPartitioner(rdd, partitionsPerDimension = 5, false, minMax, dimensions = 2)
      case "bsp" => new BSPartitioner(rdd,1,1)
        //TODO quadtree!!
    }

    rdd.partitionBy(partitioner).values
    */

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
