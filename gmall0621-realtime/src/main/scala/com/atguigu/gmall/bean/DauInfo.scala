package com.atguigu.gmall.bean

case class DauInfo(
                     mid:String,//设备id
                     uid:String,//用户id
                     ar:String,//地区
                     ch:String,//渠道
                     vc:String,//版本
                     dt:String,//日期
                     hr:String,//小时
                     mi:String,//分钟
                     ts:Long //时间戳
                   )
