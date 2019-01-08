package types

case class OArimastateGD(var history:Option[Array[STPoint]],
                         var w_lon:Option[Array[Double]],
var w_lat:Option[Array[Double]],
 var w_speed:Option[Array[Double]],
 var w_heading:Option[Array[Double]],
 var i:Option[Int]
)


/*{
  private var w_lon:Array[Double]=_
  private var w_lat:Array[Double]=_
  private var w_speed:Array[Double]=_
  private var w_heading:Array[Double]=_
  private var i:Int = _

  def getterI():Int= i

  def setterI(i:Int)=this.i=i

  def getterWLON():Array[Double]= w_lon

  def setterWLON(w_lon:Array[Double])=this.w_lon=w_lon

  def getterWLAT():Array[Double]= w_lat

  def setterWLAT(w_lat:Array[Double])=this.w_lat=w_lat


  def getterWSPEED():Array[Double]= w_speed

  def setterWSPEED(w_speed:Array[Double])=this.w_speed=w_speed

  def getterWHEADING():Array[Double]= w_heading

  def setterWHEADING(w_heading:Array[Double])=this.w_heading=w_heading
}
*/