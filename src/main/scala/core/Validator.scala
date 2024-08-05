package core

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import scala.collection.mutable.ArrayBuffer

object Validator {
  /**
   * 数据校验
   * @param title 校验主题
   * @param value 待校主题
   * @param regex
   */

    def check(title:String,value:Any,regex:String=null)={
    if (null==value){
      throw new RuntimeException(s"$title null pointer exception")
    }
    if (value.isInstanceOf[String]){
      if (value.toString.isEmpty){
        throw new RuntimeException(s"value for $title empty exception")
      }
      if (null != regex && !value.toString.matches(regex)){
        throw new RuntimeException(s"value for $title not match regex $regex exception")
      }
    }
  }

  case class DimDate(tran_year:Int,tran_quarter:Int,tran_month:Int,tran_month_week:Int,tran_day:Int,tran_range:Int)
  def dimDate(dateBegin:String,dateEnd:String):Seq[DimDate]={
    var begin = LocalDateTime.parse(s"${dateBegin}T00:00:00")
    val end = LocalDateTime.parse(s"${dateEnd}T00:00:00")
    val buffer = ArrayBuffer[DimDate]()
    while(begin.isBefore(end)){
      val year = begin.getYear
      val month = begin.getMonthValue
      val quarter = (month - 1) / 3 + 1
      val monthWeek = begin.get(ChronoField.ALIGNED_WEEK_OF_MONTH)
      val day = begin.getDayOfMonth
      val range = begin.getHour / 3 + 1
      buffer.append(DimDate(year,quarter,month,monthWeek,day,range))
      begin=begin.plusHours(1)
    }
    buffer
  }

  def dateFormat(dateTime: String)={
    val arr: Array[String] = dateTime.split(" ")
    val ymd:String = arr(0).split("-").map(_.toInt).map(d => s"${if (d < 10) "0" else ""}$d")
      .mkString("-")
    val ints: Array[Int] = arr(1).split(":").map(_.toInt)
    var bool =false
    if(ints(0)>=24){
      ints(0)-=24
      bool=true
    }
    val hms = ints.map(d => s"${if (d < 10) "0" else ""}$d")
      .mkString(":")
    val date: String = Array(ymd, hms).mkString("T")
    LocalDateTime .parse(date).plusDays(if(bool)1 else 0).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }


  def main(args: Array[String]): Unit = {
    val str = dateFormat("2024-1-3 24:10:11")
    println(str)
  }
}
