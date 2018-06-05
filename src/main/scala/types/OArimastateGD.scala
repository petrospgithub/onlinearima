package types

case class OArimastateGD(var history:Array[STPoint]) {
  private var w_lon:Array[Double]=_
  private var w_lat:Array[Double]=_
  private var i:Int = _

  def getterI():Int= i

  def setterI(i:Int)=this.i=i

  def getterWLON():Array[Double]= w_lon

  def setterWLON(w_lon:Array[Double])=this.w_lon=w_lon

  def getterWLAT():Array[Double]= w_lat

  def setterWLAT(w_lat:Array[Double])=this.w_lat=w_lat

}