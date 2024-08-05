package warehouse

import core.SparkFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import core.Validator.dateFormat

object EBSDwd {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ebs_01")
      .optimizeDriver()
      .optimizeExecutor()
      .optimizeLimit()
      .optimizeSerializer()
      .optimizeNetAbout()
      .optimizeDynamicAllocation()
      .optimizeShuffle()
      .optimizeSpeculation()
      .warehouseDir("hdfs://single01:9000/hive312/warehouse")
      .end()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.sql("create database if not exists ebs_dwd")

    /**
     * DWD:维度表加载
     * 1.用户表customer中需要根据指标列的需求进行列裁剪
     * 2.店铺表全部保留
     * 3.
     */
    //信用卡 职位 性别 国家 邮箱 编号
//    spark.table("ebs_ods.customer")
//      .select($"customer_id",$"customer_name",$"credit_no",$"job",$"gender",$"country",
//        regexp_extract($"email",".*@(.*)",1).as("email")
//      )
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_dwd.customer")


//    spark.table("ebs_ods.store")
//      .select($"store_id",$"store_name",$"employee_number")
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_dwd.store")

    /**
     * 事实表:交易表细化
     * 1.日期维度细化 年季月周日 时段(8)
     * 2.清洗重复的交易编号
     * 3.按年月分区
     * 4.将日期和时间合并成标准格式 yyyy-MM-dd HH:mm:ss
     */

      spark.udf.register("my_date_format",(datetime:String)=>dateFormat(datetime))
    val tran: DataFrame = spark.table("ebs_ods.transaction")
      .select($"transaction_id".cast(IntegerType),
        $"customer_id".cast(IntegerType),
        $"store_id".cast(IntegerType),
        $"price".cast(DecimalType(10,2)),
        $"product",
        //修正错误时间做法:先转换成时间戳(正确),然后就可以从正确时间戳解析
        callUDF("my_date_format",concat_ws(" ",$"date",$"time")) .as("tran_dt")
      )
      .cache()

    val allTran:DataFrame = tran
      .withColumn(
        "rnk",
        row_number()
          .over(Window.partitionBy($"transaction_id").orderBy($"tran_dt"))
      ).cache()

    val tranUnrepeated = allTran.where($"rnk" === 1)
      .select($"transaction_id",$"customer_id",$"store_id",$"price",$"product",$"tran_dt")
      .cache()

    val maxTranId: DataFrame = tranUnrepeated.agg(max($"transaction_id").as("max_tran_id"))
    val tranRepeated = allTran.where($"rnk" > 1)
      .withColumn("rn",
        row_number().over(Window.orderBy($"transaction_id")))
      .as("a")
      .crossJoin(maxTranId.as("M"))
      .select(($"a.rn"+$"M.max_tran_id").as("transaction_id")
        , $"customer_id", $"store_id", $"price", $"product", $"tran_dt"
      )

    //日期维度细化 年季月周日 时段(8)
    tranUnrepeated
      .unionAll(tranRepeated)
      .select($"transaction_id", $"customer_id", $"store_id", $"price", $"product", $"tran_dt",
        year($"tran_dt").as("tran_year")
        ,quarter($"tran_dt").as("tran_quarter")
        ,month($"tran_dt").as("tran_month"),
        ceil(dayofmonth($"tran_dt")/7).as("tran_month_week")
        ,dayofmonth($"tran_dt").as("tran_day")
        ,(floor(hour($"tran_dt")/3)+1).as("tran_range")
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .partitionBy("tran_year","tran_quarter","tran_month")
      .saveAsTable("ebs_dwd.transaction")

//    val review = spark.table("ebs_ods.review")
//      .cache()

    val tranDwd = spark.table("ebs_dwd.transaction")
    val review = spark.table("ebs_ods.review")
      .select(
        $"transaction_id".cast(IntegerType),
        $"store_id".cast(IntegerType),
        $"review_score".cast(DecimalType(3,2))
      )
      .as("R")
      .join(tranDwd.as("T"), Seq("transaction_id"), joinType = "left")
      .withColumn("alia_store_id",
        when($"T.store_id".isNull, $"R.store_id").otherwise($"T.store_id")
      )
      .select($"transaction_id", $"alia_store_id".as("store_id"), $"review_score")
      .cache()

    val avg_review_score = review
      .where($"review_score".isNotNull)
      .agg(
        avg($"review_score").cast(DecimalType(3,2)).as("avg_score")
      )


    review
      .join(avg_review_score)
      .withColumn("review_score",
        when($"review_score".isNull,$"avg_score")
          .otherwise($"review_score")
      )
      .select($"transaction_id",$"store_id",$"review_score")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ebs_dwd.review")

    spark.stop()
  }
}
