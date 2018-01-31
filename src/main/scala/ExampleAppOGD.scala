import java.io.{FileInputStream, InputStream}
import java.util.concurrent.ThreadLocalRandom

import onlinearima.{OARIMA_ogd, OARIMA_ons}
import org.apache.commons.math3.linear.MatrixUtils
import utils.{Normalize, Options}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
 /*
 TODO mikro lrate psaksimo gia to pws 8a problepw pio makrua
 na dokimasw diafores mhpws koitaksw makrutera
 clustering stous suntelestes
 regression???
 
  */
object ExampleAppOGD extends App{

  val filename = getClass.getResource("")
  val input: InputStream = new FileInputStream(filename.getPath)

  val source = Source.fromInputStream(input)

  var counter:Int=0

  val t_arr=new ArrayBuffer[Long]()
  val lon_arr=new ArrayBuffer[Double]()

  val lat_arr=new ArrayBuffer[Double]()
  val alt_arr=new ArrayBuffer[Double]()


  for (point_text <- source.getLines()) {
    if (counter>0) {
      val split=point_text.split(",")
      t_arr.append(split(1).toLong)
      lon_arr.append(split(2).toDouble)
      lat_arr.append(split(3).toDouble)
      alt_arr.append(split(4).toDouble)
    }
    counter+=1
  }

  //val maximum=100
  //val minimum=0

  val window=3
  val Horizon=4
  val l_rate=0.001

  val w=new Array[Double](window)
  val eye=new Array[Double](window)
  val epsilon=Math.pow(10, -0.5)
  var j=0

  var i=window+1

  while (j<window) {
    w(j)=ThreadLocalRandom.current().nextInt(0, 100+1)/100.0
    eye(j)=epsilon
    j=j+1
  }

  //println(lon_arr.toList)

  val opt:Options=Options(window,l_rate,w,eye)
/*
  val ret_ogd=OARIMA_ogd.call(lon_arr.slice(0,window).toArray, opt, lon_arr(window), i)

  println(ret_ogd)

  val ret_ons=OARIMA_ons.call(lon_arr.slice(0,window).toArray, opt, lon_arr(window), i)

  println(ret_ons)

  val diff=Normalize.diff(lon_arr.slice(0,window).toArray)

  println(lon_arr.slice(0,window))
  println(diff.toList)

  println(Normalize.reconstruct_diff(lon_arr.slice(0,window).toArray,diff).toList)

  println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
*/
  j=0

  while (j<lon_arr.size-window) {

    //val diff_t=Normalize.diff(t_arr.slice(j,j+window+1).toArray)

    /*arma - minmax normalization*/
    /*

    val mm_lon=Normalize.minmax_normalization(lon_arr.slice(j,j+window+1).toArray)
    val mm_lat=Normalize.minmax_normalization(lat_arr.slice(j,j+window+1).toArray)

    val ret_ogd_lon=OARIMA_ogd.call(mm_lon._1.slice(0,mm_lon._1.length-1), opt, mm_lon._1.last, i)
    val ret_ogd_lat=OARIMA_ogd.call(mm_lat._1.slice(0,mm_lat._1.length-1), opt, mm_lat._1.last, i)

    var h=1

    //println(((ret_ogd_lon._1*reg_lon._4)+(diff_t(1)*h*reg_lon._2)+reg_lon._3)+" "+((ret_ogd_lat._1*reg_lat._4)+(diff_t(1)*h*reg_lat._2)+reg_lat._3))

    //println((ret_ogd_lon._1*(mm_lon._3-mm_lon._2)+mm_lon._2)+" "+(ret_ogd_lat._1*(mm_lat._3-mm_lat._2)+mm_lat._2))

    val result_lon=((mm_lon._3-mm_lon._2)/2)*ret_ogd_lon._1+1+mm_lon._2
    val result_lat=((mm_lat._3-mm_lat._2)/2)*ret_ogd_lat._1+1+mm_lat._2

    //println(lon_arr(3)+" "+lat_arr(3))
    println(result_lon+" "+result_lat)

    val buf_lon=new ArrayBuffer[Double]()
    val buf_lat=new ArrayBuffer[Double]()

    buf_lon.appendAll(mm_lon._1.slice(1,mm_lon._1.length-1))
    buf_lat.appendAll(mm_lat._1.slice(1,mm_lat._1.length-1))

    buf_lon.append(ret_ogd_lon._1)
    buf_lat.append(ret_ogd_lat._1)

    //System.exit(0)
    while (h<=Horizon) {

      val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon.slice(h-1,buf_lon.size).toArray).transpose())
      val pred_lat=ret_ogd_lat._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat.slice(h-1,buf_lat.size).toArray).transpose())
      //todo!!!

      //println((pred_lon.getEntry(0,0)*(mm_lon._3-mm_lon._2)+mm_lon._2)+" "+(pred_lat.getEntry(0,0)*(mm_lat._3-mm_lat._2)+mm_lat._2))

      val result_lon=((mm_lon._3-mm_lon._2)/2)*pred_lon.getEntry(0,0)+1+mm_lon._2
      val result_lat=((mm_lat._3-mm_lat._2)/2)*pred_lat.getEntry(0,0)+1+mm_lat._2

      //println(lon_arr(3)+" "+lat_arr(3))
      println(result_lon+" "+result_lat)

      buf_lon.append(pred_lon.getEntry(0,0))
      buf_lat.append(pred_lat.getEntry(0,0))

      h=h+1
    }

  */

    /* arma - regression normalization*/
/*
    val reg_lon=Normalize.simple_regression(lon_arr.slice(j,j+window+1).toArray, diff_t)
    val reg_lat=Normalize.simple_regression(lat_arr.slice(j,j+window+1).toArray, diff_t)

    val ret_ogd_lon=OARIMA_ogd.call(reg_lon._1.slice(0,reg_lon._1.length-1), opt, reg_lon._1.last, i)
    val ret_ogd_lat=OARIMA_ogd.call(reg_lat._1.slice(0,reg_lat._1.length-1), opt, reg_lat._1.last, i)

    //println(((ret_ogd_lon._1*diff_lon._3)+diff_lon._2)+" "+((ret_ogd_lat._1*diff_lat._3)+diff_lat._2))

    var h=1

    println(((ret_ogd_lon._1*reg_lon._4)+(diff_t(1)*h*reg_lon._2)+reg_lon._3)+" "+((ret_ogd_lat._1*reg_lat._4)+(diff_t(1)*h*reg_lat._2)+reg_lat._3))


    val buf_lon=new ArrayBuffer[Double]()
    val buf_lat=new ArrayBuffer[Double]()

    buf_lon.appendAll(reg_lon._1.slice(1,reg_lon._1.length-1))
    buf_lat.appendAll(reg_lat._1.slice(1,reg_lat._1.length-1))

    buf_lon.append(ret_ogd_lon._1)
    buf_lat.append(ret_ogd_lat._1)
    //println(ret_ogd_lon._2.getRow(0).toList)
    while (h<=Horizon) {
      //println(diff_lon._1.toList)
      //println(buf_lon.slice(h-1,buf_lon.size))

      val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon.slice(h-1,buf_lon.size).toArray).transpose())
      val pred_lat=ret_ogd_lat._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat.slice(h-1,buf_lat.size).toArray).transpose())
      //todo!!!

      //println(pred_lon.getEntry(0,0)+" "+pred_lat.getEntry(0,0))

      //println(((pred_lon.getEntry(0,0)*diff_lon._3)+diff_lon._2)+" "+((pred_lat.getEntry(0,0)*diff_lat._3)+diff_lat._2))

      println(((pred_lon.getEntry(0,0)*reg_lon._4)+(diff_t(1)*h*reg_lon._2)+reg_lon._3)+" "+((pred_lat.getEntry(0,0)*reg_lat._4)+(diff_t(1)*h*reg_lat._2)+reg_lat._3))

      buf_lon.append(pred_lon.getEntry(0,0))
      buf_lat.append(pred_lat.getEntry(0,0))

      h=h+1
      //System.exit(1)
    }
*/
    /* arma - gaussian normalization*/
//todo mean/std online! split into training and test set!!!
/*

//sto normalize na min bazw kai ta 4 shmeia!!!
//todo shift data!!!
    val diff_lon=Normalize.gaussian_normalization(lon_arr.slice(j,j+window).toArray)
    val diff_lat=Normalize.gaussian_normalization(lat_arr.slice(j,j+window).toArray)

    var kati_counter=0
/*
    while (kati_counter<diff_lon._1.length) {

      println((diff_lon._1(kati_counter)*diff_lon._3)+diff_lon._2+" "+((diff_lat._1(kati_counter)*diff_lat._3)+diff_lat._2))
      println(lon_arr(j+kati_counter)+" "+lat_arr(j+kati_counter))


      kati_counter=kati_counter+1
    }
*/
    println("training "+diff_lon._1.toList)



    //println("training: "+diff_lon._1.slice(0,diff_lon._1.length-1).toList)

    val real_norm_lon=(lon_arr(j+window)-diff_lon._2)/diff_lon._3
    val real_norm_lat=(lat_arr(j+window)-diff_lat._2)/diff_lat._3

    val ret_ogd_lon=OARIMA_ogd.call(diff_lon._1, opt, real_norm_lon, i)
    val ret_ogd_lat=OARIMA_ogd.call(diff_lat._1, opt, real_norm_lat, i)

    //println(lon_arr(j+window))
    //println(diff_lon._1.toList)
    //println(diff_lon)
    //println(ret_ogd_lon._1)
    val result_lon=(ret_ogd_lon._1*diff_lon._3)+diff_lon._2
    val result_lat=(ret_ogd_lat._1*diff_lat._3)+diff_lat._2

    println(diff_lon._2)
    println(diff_lon._3)

    println(real_norm_lon)

    println(ret_ogd_lon._1)
    //println(result_lon+" "+result_lat)

    //System.exit(1)

    var h=1

    val buf_lon=new ArrayBuffer[Double]()
    val buf_lat=new ArrayBuffer[Double]()


    buf_lon.appendAll(diff_lon._1.slice(1,diff_lon._1.length))
    buf_lat.appendAll(diff_lat._1.slice(1,diff_lat._1.length))

    buf_lon.append(ret_ogd_lon._1)
    buf_lat.append(ret_ogd_lat._1)

    //buf_lon.appendAll(lon_arr.slice(j+1,j+window+1))
    //buf_lat.appendAll(lat_arr.slice(j+1,j+window+1))

    //println(buf_lon)

    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    System.exit(0)
    while (h<=Horizon) {
      //println(diff_lon._1.toList)
      //println(buf_lon.slice(h-1,buf_lon.size))

      val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon.slice(h-1,buf_lon.size).toArray).transpose())
      val pred_lat=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat.slice(h-1,buf_lat.size).toArray).transpose())
      //todo!!!
      //ta bgazei stin mesh!!!
      //println(pred_lon.getEntry(0,0)+" "+pred_lat.getEntry(0,0))

      //println(((pred_lon.getEntry(0,0)*diff_lon._3)+diff_lon._2)+" "+((pred_lat.getEntry(0,0)*diff_lat._3)+diff_lat._2))

      buf_lon.append(pred_lon.getEntry(0,0))
      buf_lat.append(pred_lat.getEntry(0,0))

      h=h+1
      //System.exit(1)
    }
 */

    /* arima - diff*/
/*
    val diff_lon=Normalize.diff(lon_arr.slice(j,j+window+1).toArray)
    val diff_lat=Normalize.diff(lon_arr.slice(j,j+window+1).toArray)

    val ret_ogd_lon=OARIMA_ogd.call(diff_lon.slice(0,diff_lon.length-1), opt, diff_lon.last, i)
    val ret_ogd_lat=OARIMA_ogd.call(diff_lat.slice(0,diff_lon.length-1), opt, diff_lat.last, i)

    println((ret_ogd_lon._1+lon_arr(i-1))+" "+(ret_ogd_lat._1+lat_arr(i-1)))

    var h=1

    val buf_lon=new ArrayBuffer[Double]()
    val buf_lat=new ArrayBuffer[Double]()

    buf_lon.appendAll(lon_arr.slice(j+1,window+j))
    buf_lat.appendAll(lat_arr.slice(j+1,window+j))

    buf_lon.append(ret_ogd_lon._1)
    buf_lat.append(ret_ogd_lat._1)

    while (h<=Horizon) {

      val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon.slice(h-1,buf_lon.size).toArray).transpose())
      val pred_lat=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat.slice(h-1,buf_lat.size).toArray).transpose())

      println((pred_lon.getEntry(0,0)+buf_lon.last)+" "+(pred_lat.getEntry(0,0)+buf_lat.last))

      buf_lon.append(pred_lon.getEntry(0,0))
      buf_lat.append(pred_lat.getEntry(0,0))

      h=h+1

    }

    */

    /*arma*/
    /**/
    val ret_ogd_lon=OARIMA_ogd.call(lon_arr.slice(j,j+window).toArray, opt, lon_arr(window), i)
    val ret_ogd_lat=OARIMA_ogd.call(lat_arr.slice(j,j+window).toArray, opt, lat_arr(window), i)
    println(ret_ogd_lon._1+" "+ret_ogd_lat._1)

    var h=1

    val buf_lon=new ArrayBuffer[Double]()
    val buf_lat=new ArrayBuffer[Double]()

    buf_lon.appendAll(lon_arr.slice(j+1,window+j)) //check!!!
    buf_lat.appendAll(lat_arr.slice(j+1,window+j))

    buf_lon.append(ret_ogd_lon._1)
    buf_lat.append(ret_ogd_lat._1)

    while (h<=Horizon) {

      val pred_lon=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lon.slice(h-1,buf_lon.size).toArray).transpose())
      val pred_lat=ret_ogd_lon._2.multiply(MatrixUtils.createRowRealMatrix(buf_lat.slice(h-1,buf_lat.size).toArray).transpose())

      println(pred_lon.getEntry(0,0)+" "+pred_lat.getEntry(0,0))

      buf_lon.append(pred_lon.getEntry(0,0))
      buf_lat.append(pred_lat.getEntry(0,0))

      h=h+1

    }
    /**/
    j=j+1
    i=i+1
  }
}
