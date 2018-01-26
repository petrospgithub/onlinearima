package utils

import org.apache.commons.math3.stat.regression.SimpleRegression

object Normalize {
  def diff(arr:Array[Double]): Array[Double] = {
    val ret:Array[Double]=new Array[Double](arr.length)

    ret(0)=0

    var j=1

    while (j<arr.length) {
      ret(j)=arr(j)-arr(j-1)
      j=j+1
    }
    ret
  }

  def diff(arr:Array[Long]): Array[Double] = {
    val ret:Array[Double]=new Array[Double](arr.length)

    ret(0)=0

    var j=1

    while (j<arr.length) {
      ret(j)=arr(j)-arr(j-1)
      j=j+1
    }
    ret
  }

  def minmax_normalization(arr:Array[Double]): (Array[Double],Double, Double) ={

    val n=arr.length

    val min=arr.min
    val max=arr.max

    val ret:Array[Double]=new Array[Double](arr.length)

    var j=0

    while (j<n) {
      ret(j)=((2/(max-min)) * (arr(j)-min) )-1
      j=j+1
    }

    (ret,min,max)
  }

  def gaussian_normalization(arr:Array[Double]): (Array[Double],Double, Double) ={

    val n=arr.length

    val mean=arr.sum/n

    var s=0.0

    var j=0

    while (j<n) {

      s=s+Math.pow(arr(j)-mean,2)

      j=j+1
    }

    val std=Math.sqrt(s/n)

    val ret:Array[Double]=new Array[Double](n)

    j=0

    while (j<n) {
      ret(j)=(arr(j)-mean)/std
      j=j+1
    }

    (ret,mean,std)
  }

  def simple_regression(arr:Array[Double], t:Array[Double]): (Array[Double],Double, Double, Double) ={
    val n=arr.length

    var j=0
    val mean=arr.sum/n

    var s=0.0

    val regression = new SimpleRegression()

    while (j<n) {
      regression.addData(t(j), arr(j))

      s=s+Math.pow(arr(j)-mean,2)

      j=j+1
    }

    val std=Math.sqrt(s/n)
    val ret:Array[Double]=new Array[Double](n)

    j=0

    while (j<n) {
      ret(j)=(arr(j)-( regression.getSlope*t(j)+regression.getIntercept ) ) / std
      j=j+1
    }

    (ret,regression.getSlope,regression.getIntercept, std)
  }

  def reconstruct_diff(arr:Array[Double], diff: Array[Double]): Array[Double] = {
    val ret=new Array[Double](arr.length)

    ret(0)=arr.head

    var j=1

    while (j<diff.length) {

      ret(j)=diff(j)+arr(j-1) //todo + or -

      j=j+1
    }

    ret
  }

  def reconstruct_gaussian(arr:Array[Double], mean:Double, std:Double): Array[Double] = {
    val ret=new Array[Double](arr.length)

    var j=0

    while (j<arr.length) {

      ret(j)=(arr(j)*std)+mean

      j=j+1
    }

    ret
  }

  def reconstruct_regression(arr:Array[Double], t:Array[Double], slope:Double, intercept:Double, std:Double): Array[Double] = {
    val ret=new Array[Double](arr.length)

    var j=0

    while (j<arr.length) {

      ret(j)=(arr(j)*std)+(t(j)*slope)+intercept

      j=j+1
    }

    ret
  }
}
