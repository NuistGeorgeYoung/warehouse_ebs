import org.apache.spark.sql.SparkSession

object test {
  case class ChatRecord(
     localId: Long,
     TalkerId: Long,
     Type: String,
     SubType: String,
     IsSender: Boolean,
     CreateTime: String,
     Status: String,
     StrContent: String,
     StrTime: String,
     Remark: String,
     NickName: String,
     Sender: String)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("sparkhive")
      .master("local[*]")
      .config("hive.metastore.uris","thrift://single01:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
//    val schema:StructType = StructType(
//      Seq(
//        StructField("localid",LongType),
//        StructField("TalkerId",IntegerType),
//        StructField("v_type",IntegerType),
//        StructField("v_subtype",IntegerType),
//        StructField("issender",IntegerType),
//        StructField("createtime",StringType),
//        StructField("status",IntegerType),
//        StructField("strcontext",StringType),
//        StructField("strtime",StringType),
//        StructField("remark",StringType),
//        StructField("nickname",StringType),
//        StructField("sender",StringType)
//      )
//    )


    //localId	TalkerId	Type	SubType	IsSender	CreateTime	Status	StrContent	StrTime	Remark	NickName	Sender


    // 关闭会话
//    spark.sql("select *from we_chat_analysis.zhangxiu limit 10;").show(100)

    //求各个时间段的聊天记录数
    spark.sql("""
      SELECT
        CASE
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 2 AND 5 THEN '凌晨2点至6点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 6 AND 9 THEN '6点至10点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 10 AND 13 THEN '10点至14点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 14 AND 17 THEN '14点至18点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 18 AND 21 THEN '18点至22点'
          ELSE '22点至次日凌晨2点'
        END AS TimeSlot,
        COUNT(*) AS ChatCount
      FROM
        we_chat_analysis.zhangxiu
      GROUP BY
        CASE
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 2 AND 5 THEN '凌晨2点至6点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 6 AND 9 THEN '6点至10点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 10 AND 13 THEN '10点至14点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 14 AND 17 THEN '14点至18点'
          WHEN hour(from_unixtime(unix_timestamp(StrTime, 'yyyy-MM-dd HH:mm:ss'))) BETWEEN 18 AND 21 THEN '18点至22点'
          ELSE '22点至次日凌晨2点'
        END
      ORDER BY
        TimeSlot
    """).show()

    // 读取Hive表

    val chatDF = spark.sql("SELECT * FROM we_chat_analysis.zhangxiu")

    // 查询凌晨2点到6点的记录
    val earlyMorningChatsDF = chatDF.filter(
      hour(unix_timestamp($"StrTime", "yyyy-MM-dd HH:mm:ss").cast("timestamp")) >= 2 &&
        hour(unix_timestamp($"StrTime", "yyyy-MM-dd HH:mm:ss").cast("timestamp")) <= 5
    )
    earlyMorningChatsDF.show()

    spark.close()

  }
}
