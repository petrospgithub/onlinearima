package onlinearima

import org.apache.commons.math3.linear.{MatrixUtils, RealMatrix}
import utils.Options

object OARIMA_ons {

  def adapt_w(prediction:Double, real:Double, w:Array[Double], lrate:Double, A_trans:RealMatrix, matrix:RealMatrix, curr_position:Int): (Array[Double], RealMatrix) = {
    val w_matrix= MatrixUtils.createRowRealMatrix(w)
    val window=w.length
    val grad=new Array[Double](window)
    val diff=prediction-real
    var j=0
    //val eye=new Array[Double](window)

    while (j<grad.length) {
      grad(j)=2*matrix.getEntry(0,j)*diff
      j=j+1
    }

    val grad_matrix=MatrixUtils.createRowRealMatrix(grad)
    //val A_trans=MatrixUtils.createRealDiagonalMatrix(eye)

    val var_1=A_trans.multiply(grad_matrix.transpose())
      .multiply(grad_matrix)
      .multiply(A_trans)

    val var_2=grad_matrix
      .multiply(A_trans)
      .multiply(grad_matrix.transpose()).getEntry(0,0)+1

    var m=0

    while (m<var_1.getRowDimension){
      var n=0
      while (n<var_1.getColumnDimension) {
        A_trans.setEntry(m,n, A_trans.getEntry(m,n)-var_1.getEntry(m,n)/var_2)
        n=n+1
      }
      m=m+1
    }

    val correction_matrix=MatrixUtils.createRowRealMatrix(grad)
      .multiply(A_trans)

    j=0

    while (j<window) {
      w_matrix.setEntry(0,j,w_matrix.getEntry(0,j) - correction_matrix.getEntry(0,j)*lrate )
      j=j+1
    }

    (w_matrix.getRow(0), A_trans)
  }

  def prediction(arr:Array[Double], w:Array[Double]): (Double, RealMatrix) = {
    val matrix = MatrixUtils.createRowRealMatrix(arr)
    val w_matrix= MatrixUtils.createRowRealMatrix(w)

    val prediction=w_matrix.multiply(matrix.transpose()).getEntry(0,0)

    (prediction, matrix)
  }

  def call(lon_arr:Array[Double], opt:Options, real_next:Double, curr_position:Int): (Double, RealMatrix) = {
    val lon_matrix = MatrixUtils.createRowRealMatrix(lon_arr)
    val w_matrix= MatrixUtils.createRowRealMatrix(opt.w)

    val lon_prediction=w_matrix.multiply(lon_matrix.transpose()).getEntry(0,0)

    val diff=lon_prediction-real_next

    val grad=new Array[Double](opt.window)

    var j=0

    while (j<grad.length) {
      grad(j)=2*lon_matrix.getEntry(0,j)*diff
      j=j+1
    }

    val grad_matrix=MatrixUtils.createRowRealMatrix(grad)
    val A_trans=MatrixUtils.createRealDiagonalMatrix(opt.eye)

    val var_1=A_trans.multiply(grad_matrix.transpose())
      .multiply(grad_matrix)
      .multiply(A_trans)

    val var_2=grad_matrix
      .multiply(A_trans)
      .multiply(grad_matrix.transpose()).getEntry(0,0)+1

    var m=0

    while (m<var_1.getRowDimension){
      var n=0
      while (n<var_1.getColumnDimension) {
        A_trans.setEntry(m,n, A_trans.getEntry(m,n)-var_1.getEntry(m,n)/var_2)
        n=n+1
      }
      m=m+1
    }

    val correction_matrix=MatrixUtils.createRowRealMatrix(grad)
      .multiply(A_trans)

    j=0

    while (j<opt.window) {
      w_matrix.setEntry(0,j,w_matrix.getEntry(0,j) - correction_matrix.getEntry(0,j)*opt.l_rate )
      j=j+1
    }

    (lon_prediction,w_matrix)

  }

  /*
  val lon_arr=Array(2.1111, 2.1150, 2.1188)

  println(lon_arr.toList)

  val real_lon_next=2.1227

  val maximum=100
  val minimum=0

  val window=3

  val l_rate=1000
  val epsilon=Math.pow(10, -0.5)

  println(epsilon)

  val w=new Array[Double](window)
  val eye=new Array[Double](window) //TODO!!!
  var j=0

  val i=window+1

  while (j<window) {
    w(j)=ThreadLocalRandom.current().nextInt(minimum, maximum+1)/100.0 //TODO check minimum maximum
    eye(j)=epsilon
    j=j+1
  }

  println(w)

  val lon_m = MatrixUtils.createRowRealMatrix(lon_arr)
  val w_matrix= MatrixUtils.createRowRealMatrix(w)

  println(lon_m)
  println("init_w: "+w_matrix)

  val lon_prediction=w_matrix.multiply(lon_m.transpose()).getEntry(0,0)

  println("prediction: "+lon_prediction)

  val diff=lon_prediction-real_lon_next

  println("diff: " +diff)

  val grad=new Array[Double](window)

  j=0

  while (j<grad.length) {
    grad(j)=2*lon_m.getEntry(0,j)*diff
    j=j+1
  }

  println("grad: "+grad.toList)

  val grad_matrix=MatrixUtils.createRowRealMatrix(grad)
  val A_trans=MatrixUtils.createRealDiagonalMatrix(eye)

  println("trans: "+A_trans)

  val var_1=A_trans.multiply(grad_matrix.transpose())
                .multiply(grad_matrix)
                  .multiply(A_trans)

  println("var_1: "+var_1)

  val var_2=grad_matrix
    .multiply(A_trans)
    .multiply(grad_matrix.transpose()).getEntry(0,0)+1

  println("var_2: "+var_2)

  var m=0

  while (m<var_1.getRowDimension){
    var n=0
    while (n<var_1.getColumnDimension) {
      //println(m)
      //println(n)
      A_trans.setEntry(m,n, A_trans.getEntry(m,n)-var_1.getEntry(m,n)/var_2)
      n=n+1
    }
    m=m+1
  }

  println(A_trans)

  val correction_matrix=MatrixUtils.createRowRealMatrix(grad)
    .multiply(A_trans)

  println(correction_matrix)


  j=0

  while (j<window) {
    //todo check lrate...
    println((lon_m.getEntry(0,j)*2*diff/Math.sqrt(i - window)) * l_rate)

    w_matrix.setEntry(0,j,w_matrix.getEntry(0,j) - correction_matrix.getEntry(0,j)*l_rate )
    j=j+1
  }

  println(w_matrix)
*/
}
