package com.parqduetUtils

object appUtils {
  //判断原始请求，有效请求，广告请求
  def request(requestmode:Int,processnode:Int):List[Double]={
    if (requestmode==1 && processnode==1){//原始请求只是原始请求
      List[Double](1,0,0)
    }else if (requestmode==1 && processnode==2){//有效请求也是原始请求
      List[Double](1,1,0)
    }else if (requestmode==1 && processnode==3){//广告请求既是原始也是有效
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  //竞价数，竞价成功数，广告成本费和消费
  def Bidding(iseffective:Int, isbilling:Int, isbid:Int,iswin:Int,adorderid:Int,adpayment: Double,winprice: Double):List[Double]={
    if(iseffective==1 && isbilling==1 && isbid==1){
      if (iswin==1 && adorderid!=0){
        List[Double](1,1,adpayment/1000,winprice/1000)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
  //展示数，点击数
  def Click(requestmode:Int ,iseffective:Int):List[Double]={
    if (requestmode==2 && iseffective==1){
      List[Double](1,0)
    }else if (requestmode==3 && iseffective==1){
      List[Double](1,1)
    }else {
      List[Double](0,0)
    }
  }

}
