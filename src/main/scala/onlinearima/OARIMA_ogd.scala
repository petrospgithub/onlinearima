package onlinearima

import org.apache.commons.math3.linear.MatrixUtils
  //import utils.Options

object OARIMA_ogd {

  def adapt_w(prediction:Double, real:Double, w:Array[Double], lrate:Double, data:Array[Double], curr_position:Int): Array[Double] = {
    val matrix=MatrixUtils.createRowRealMatrix(data.reverse)
    val w_matrix= MatrixUtils.createRowRealMatrix(w)
    val diff=prediction-real

   // println("diff: "+diff)
   // println("i: "+curr_position)

    //println(matrix)
    //println(w_matrix)

    var j=0

    while (j<w.length) {

      //println( (matrix.getEntry(0,j)*2*diff/Math.sqrt(curr_position - w.length)) )

      //todo pws paizw me to current position!!!

//     println("matrix.getEntry(0,j): "+matrix.getEntry(0,j))
      //println(Math.sqrt(curr_position - w.length))
     // println(2*diff/Math.sqrt(curr_position - w.length))


      w_matrix.setEntry(0, j, w_matrix.getEntry(0,j) - (matrix.getEntry(0,j)*2*diff/Math.sqrt(curr_position - w.length)) * lrate )
      j=j+1
    }

    //println(w_matrix)

    w_matrix.getRow(0)
  }


  def adapt_w_diff(prediction:Double, real:Double, w:Array[Double], lrate:Double, data:Array[Double], curr_position:Int): (Array[Double],Double) = {
    val matrix=MatrixUtils.createRowRealMatrix(data.reverse)
    val w_matrix= MatrixUtils.createRowRealMatrix(w)
    val diff=prediction-real

    //println(diff)
    //println(lrate)
    var j=0

    while (j<w.length) {

      //println( (matrix.getEntry(0,j)*2*diff/Math.sqrt(curr_position - w.length)) )

      //todo pws paizw me to current position!!!

      // println(matrix.getEntry(0,j))
      //println(Math.sqrt(curr_position - w.length))
      // println(2*diff/Math.sqrt(curr_position - w.length))


      w_matrix.setEntry(0, j, w_matrix.getEntry(0,j) - (matrix.getEntry(0,j)*2*diff/Math.sqrt(curr_position - w.length)) * lrate )
      j=j+1
    }

    //println(w_matrix)

    (w_matrix.getRow(0),diff)
  }

  def prediction(arr:Array[Double], w:Array[Double]): Double= {
    val matrix = MatrixUtils.createRowRealMatrix(arr.reverse)
    val w_matrix= MatrixUtils.createRowRealMatrix(w)

    val prediction=w_matrix.multiply(matrix.transpose()).getEntry(0,0)
/*
    println("matrix: "+matrix)
    println("w_matrix: "+w_matrix)
    println("prediction: "+prediction)
    println("prediction2: "+w_matrix.multiply(MatrixUtils.createRowRealMatrix(arr.reverse).transpose()).getEntry(0,0))
*/
    //(prediction, matrix)
    prediction
  }

  def prediction(arr:Array[Double], w:Array[Double], diff:Double): Double= {
    val matrix = MatrixUtils.createRowRealMatrix(arr.reverse)
    val w_matrix= MatrixUtils.createRowRealMatrix(w)

    val prediction=w_matrix.multiply(matrix.transpose()).getEntry(0,0)

    //(prediction, matrix)
    //prediction

    if (diff>0)
      prediction-diff
    else {
      prediction+diff
    }
  }

/*
  def call(lon_arr:Array[Double], opt:Options, real_next:Double, curr_position:Int): (Double, RealMatrix) = {
    val lon_matrix = MatrixUtils.createRowRealMatrix(lon_arr)
    val w_matrix= MatrixUtils.createRowRealMatrix(opt.w)

    //println(opt.w.toList)
    //println(lon_arr.toList)

    val lon_prediction=w_matrix.multiply(lon_matrix.transpose()).getEntry(0,0)

    val diff=lon_prediction-real_next

    var j=0

    while (j<opt.window) {
      w_matrix.setEntry(0,j,w_matrix.getEntry(0,j)- (lon_matrix.getEntry(0,j)*2*diff/Math.sqrt(curr_position - opt.window)) * opt.l_rate )
      j=j+1
    }

    (lon_prediction,w_matrix)
  }
  */
/*
  val lon_arr=Array(2.1111, 2.1150, 2.1188)

  println(lon_arr.toList)

  val real_lon_next=2.1227

  val maximum=100
  val minimum=0

  val window=3

  val l_rate=1000

  val w=new Array[Double](window) //TODO!!!

  var j=0

  val i=window+1

  while (j<window) {
    w(j)=ThreadLocalRandom.current().nextInt(minimum, maximum+1)/100.0 //TODO check minimum maximum
    j=j+1
  }

  println(w)

  val lon_matrix = MatrixUtils.createRowRealMatrix(lon_arr)
  val w_matrix= MatrixUtils.createRowRealMatrix(w)

  println(lon_matrix)
  println("w_matrix: "+w_matrix)

  val lon_prediction=w_matrix.multiply(lon_matrix.transpose()).getEntry(0,0)

  println("prediction: "+lon_prediction)

  val diff=lon_prediction-real_lon_next

  j=0

  while (j<window) {
    //todo check lrate...
    println((lon_matrix.getEntry(0,j)*2*diff/Math.sqrt(i - window)) * l_rate)

    w_matrix.setEntry(0,j,w_matrix.getEntry(0,j)- (lon_matrix.getEntry(0,j)*2*diff/Math.sqrt(i - window)) * l_rate )
    j=j+1
  }

  println(w_matrix)
*/
}
