package utils

import SparkStreamingSQL.Point
import org.apache.commons.lang3.SerializationUtils
import points.AvroPoint

import scala.collection.mutable.ArrayBuffer

object Copy {
  def deepCopy(source: Array[AvroPoint]): ArrayBuffer[AvroPoint] = {
    val result: ArrayBuffer[AvroPoint] = new ArrayBuffer[AvroPoint]()

    for (i <- source.indices) {
      result += SerializationUtils.clone(source(i))
    }
    result
  }

  def deepCopy(source: Array[Point]): ArrayBuffer[Point] = {
    val result: ArrayBuffer[Point] = new ArrayBuffer[Point]()

    for (i <- source.indices) {
      result += SerializationUtils.clone(source(i))
    }
    result
  }
}
