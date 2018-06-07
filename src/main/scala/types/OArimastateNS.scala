package types

import org.apache.commons.math3.linear.RealMatrix

case class OArimastateNS(var history:Array[STPoint]) {
  private var w_lon:Array[Double]=_
  private var w_lat:Array[Double]=_
  private var A_TransLON:RealMatrix=_
  private var A_TransLAT:RealMatrix=_

  //private var i:Int = _


  def getterATransLON():RealMatrix= A_TransLON

  def setterATransLON(A_TransLON:RealMatrix)=this.A_TransLON=A_TransLON

  def getterATransLAT():RealMatrix= A_TransLAT

  def setterATransLAT(A_TransLAT:RealMatrix)=this.A_TransLAT=A_TransLAT

  def getterWLON():Array[Double]= w_lon

  def setterWLON(w_lon:Array[Double])=this.w_lon=w_lon

  def getterWLAT():Array[Double]= w_lat

  def setterWLAT(w_lat:Array[Double])=this.w_lat=w_lat
}
