package onlinearima

import java.util.concurrent.ThreadLocalRandom

object RandomInit {
  def create_w(h:Int):Array[Double] ={
    val w=new Array[Double](h)

    var j=0

    while (j<h) {
      w(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0

      j=j+1
    }
    w
  }
}
