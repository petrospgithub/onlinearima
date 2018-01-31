package evaluation

import org.apache.spark.sql.SparkSession

object Readparquet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("PredictionEvaluation").master("local[*]")
      .getOrCreate()

    val df=spark.read.parquet(System.getProperty("user.dir") +"/predictions/"+ "*.parquet")

    df.printSchema()
    df.orderBy("timestamp").show()
    //df.show()
  }
}
