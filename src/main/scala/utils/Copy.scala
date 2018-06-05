package utils

import org.apache.commons.lang3.SerializationUtils
import types.STPoint

import scala.collection.mutable.ArrayBuffer

object Copy {
  def deepCopy(source: Array[STPoint]): Array[STPoint] = {
    val result: Array[STPoint] = new Array[STPoint](source.length)

    for (i <- source.indices) {
      result(i) = SerializationUtils.clone(source(i))
    }
    result
  }
}