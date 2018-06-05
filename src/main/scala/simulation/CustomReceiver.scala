package simulation

import java.io.{FileInputStream, InputStream}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

/**
  * Created by ppetrou on 5/20/17.
  */
class CustomReceiver(val file:String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    new Thread("Custom Receiver") {
      override def run() {
        Thread.sleep(1000)
        receive() }
    }.start()
  }

  override def onStop(): Unit = {
    stop("I am done")
  }

  private def receive() {

    var line:String= null

    var counter:Int=0
    //val filename = getClass.getResource(path)
    val input: InputStream = new FileInputStream(file)

    val source = Source.fromInputStream(input)
    for (point_text <- source.getLines()) {
      if (counter>0) {
        store(point_text)
      }
      counter+=1
      //Thread.sleep(2)
    }

    //br.close()
  }

}
