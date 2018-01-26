package SparkStreaming

import org.apache.commons.math3.linear.RealMatrix
import points.AvroPoint

import scala.collection.mutable.ArrayBuffer

case class OARIMAstate (state:ArrayBuffer[AvroPoint],
                        lon:RealMatrix,
                        lat:RealMatrix,
                        w_lon:Array[Double],
                        w_lat:Array[Double],
                        i:Int, prediction:AvroPoint,
                        real:AvroPoint)

case class OARIMAstate_ons(state: ArrayBuffer[AvroPoint],
                           lon: RealMatrix,
                           lat: RealMatrix,
                           A_trans_lon: RealMatrix,
                           A_trans_lat: RealMatrix,
                           w_lon: Array[Double],
                           w_lat: Array[Double],
                           i: Int, prediction: AvroPoint,
                           real: AvroPoint)