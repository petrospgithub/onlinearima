package types

import org.apache.commons.math3.linear.RealMatrix

case class OArimastateNS(var history:Option[Array[STPoint]],
                         var w_lon:Option[Array[Double]],
 var w_lat:Option[Array[Double]],
 var A_TransLON:Option[RealMatrix],
 var A_TransLAT:Option[RealMatrix],

 var w_speed:Option[Array[Double]],
 var w_heading:Option[Array[Double]],

 var A_TransSPEED:Option[RealMatrix],
 var A_TransHEADING:Option[RealMatrix]
)
/*
{
  private var w_lon:Array[Double]=_
  private var w_lat:Array[Double]=_
  private var A_TransLON:RealMatrix=_
  private var A_TransLAT:RealMatrix=_

  private var w_speed:Array[Double]=_
  private var w_heading:Array[Double]=_

  private var A_TransSPEED:RealMatrix=_
  private var A_TransHEADING:RealMatrix=_

  //private var i:Int = _


  def getterATransLON():RealMatrix= A_TransLON

  def setterATransLON(A_TransLON:RealMatrix)=this.A_TransLON=A_TransLON

  def getterATransLAT():RealMatrix= A_TransLAT

  def setterATransLAT(A_TransLAT:RealMatrix)=this.A_TransLAT=A_TransLAT

  def getterWLON():Array[Double]= w_lon

  def setterWLON(w_lon:Array[Double])=this.w_lon=w_lon

  def getterWLAT():Array[Double]= w_lat

  def setterWLAT(w_lat:Array[Double])=this.w_lat=w_lat


  /******************************************/

  def getterWSPEED():Array[Double]= w_speed

  def setterWSPEED(w_speed:Array[Double])=this.w_speed=w_speed

  def getterWHEADING():Array[Double]= w_heading

  def setterWHEADING(w_heading:Array[Double])=this.w_heading=w_heading

  /****************************************/


  def getterATransSPEED():RealMatrix= A_TransSPEED

  def setterATransSPEED(A_TransSPEED:RealMatrix)=this.A_TransSPEED=A_TransSPEED

  def getterATransHEADING():RealMatrix= A_TransHEADING

  def setterATransHEADING(A_TransHEADING:RealMatrix)=this.A_TransHEADING=A_TransHEADING



}
*/