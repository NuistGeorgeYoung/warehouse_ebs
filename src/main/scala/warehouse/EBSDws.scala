package warehouse

import java.util.Date

import core.{SparkFactory, Validator}
import core.Validator.dimDate
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EBSDws {
  case class Review(review_score:Int)
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
/*
    spark.sql(
      """
        |select
        |grouping__id as gid,country,gender,credit_type,count(customer_id) as agg_cnt
        |from ebs_dwd.customer
        |group by country,gender,credit_type
        |grouping sets(credit_type,(gender,country),(country,gender,credit_type))
        |""".stripMargin)
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("gid")
        .saveAsTable("ebs_dws.customer_by_cgc")

    spark
      .table("ebs_dwd.customer")
      .groupBy("job")
      .agg(count("customer_id").as("job_cnt"))
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.job")

    spark
        .table("ebs_dwd.customer")
        .groupBy("email")
        .agg(count("customer_id").as("email_cnt"))
        .repartition(1)
        .write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .saveAsTable("ebs_dws.customer_by_email")

    val rowDt = spark
      .table("ebs_dwd.transaction")
      .agg(
        min(to_date($"tran_dt")) as "min_date",
        date_add(max(to_date($"tran_dt")),1) as "max_date"
      )
        .take(1)(0)
    val minDate = rowDt.getAs[Date]("min_date").toString
    val maxDate = rowDt.getAs[Date]("max_date").toString

    spark
        .createDataFrame(dimDate(minDate,maxDate))
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable("ebs_dws.dim_date")


    val date: DataFrame = spark.table("ebs_dws.dim_date")

    spark
        .table("ebs_dwd.transaction")
        .as("tran")
        .join(date.as("dim"),Seq(
          "tran_year","tran_quarter","tran_month","tran_month_week","tran_day","tran_range"
        ),"right")
        .rollup("tran_year","tran_quarter","tran_month","tran_month_week","tran_day","tran_range")
        .agg(
          grouping_id() as "gid",
          sum($"price").cast(DecimalType(10,2)) as "sum_amount",
          avg($"price").cast(DecimalType(10,2)) as "avg_amount",
          count($"price") as "tran_count"
        )
        .withColumn("sum_amount",
          when($"sum_amount".isNull,0).otherwise($"sum_amount")
        )
        .withColumn("avg_amount",
          when($"avg_amount".isNull,0).otherwise($"avg_amount")
        )
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable("ebs_dws.tran_dim_date")
    // 如果需要，可以将结果写入Hive表
//    groupedTransactionsDF.write.mode("overwrite").saveAsTable("dw_retail_dws.transaction_date_grouped")
//    spark
//        .createDataFrame(dimDate(""))

    spark
        .sql(
          """
            |with dim_year_month as(
            | select
            |  tran_year,tran_month
            |  from ebs_dws.dim_date
            |  group by tran_year,tran_month
            |),
            |dim_year_month_customer as(
            | select
            |  *
            |  from ebs_dwd.customer
            |  cross join dim_year_month as D
            |)
            |select
            | grouping__id as gid,
            | tran_year,tran_month,customer_id,
            | cast(sum(price) as decimal(10,2)) as sum_amount,
            | cast(avg(price) as decimal(10,2)) as avg_amount,
            | count(price) as tran_count
            | from ebs_dwd.transaction
            | right join dim_year_month_customer
            | using(tran_year,tran_month,customer_id)
            | group by tran_year,tran_month,customer_id
            | grouping sets(customer_id,(tran_year,tran_month,customer_id))
            |""".stripMargin)
            .withColumn("sum_amount",
              when($"sum_amount".isNull,0).otherwise($"sum_amount")
            )
            .withColumn("avg_amount",
              when($"avg_amount".isNull,0).otherwise($"avg_amount")
            )
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .format("orc")
            .partitionBy("tran_year","tran_month")
            .saveAsTable("ebs_dws.transaction_by_ymc")

*/

    val customer = spark.table("ebs_dwd.customer")
    val bcDf = spark.sparkContext.broadcast(customer)

    val tran = spark
      .table("ebs_dwd.transaction")
        .cache()
//    tran
//        .as("T")
//        .join(bcDf.value.as("C"),Seq("customer_id"),"inner")
//        .groupBy("C.country")
//        .agg(
//          sum($"T.price").cast(DecimalType(10,2)) as "sum_amount",
//          avg($"T.price").cast(DecimalType(10,2)) as "avg_amount",
//          count($"T.transaction_id") as "tran_count",
//          size(collect_set("customer_id")).as("uq_customer_count")
//        )
//        .repartition(1)
//        .write
//        .mode(SaveMode.Overwrite)
//        .format("orc")
//        .saveAsTable("ebs_dws.transaction_by_country")

//    tran
//      .rollup("tran_year","tran_quarter","tran_month","tran_month_week")
//      .agg(
//        size(collect_set("customer_id")).as("uq_customer_count"),
//        grouping_id().as("gid")
//      )
//      .repartition(1)
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_dws.uq_customer_count_by_qw")

    val avgMonthCount = tran.groupBy("product", "tran_month")
      .agg(count("transaction_id").cast(DecimalType(10,2)).as("month_count"))
      .groupBy("product")
      .agg(avg("month_count").cast(DecimalType(10,2)).as("avg_month_count"))

/*
    tran
      .groupBy("product")
      .agg(
        sum($"price").cast(DecimalType(10,2)).as("sum_amount"),
        size(collect_set("customer_id")).as("uq_customer_count"),
        count("transaction_id").as("tran_count"),
      )
      .join(avgMonthCount,Seq("product"),"inner")
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.tran_product")
    */
//    tran
//      .groupBy("store_id")
//      .agg(
//        count("transaction_id").as("customer_count"),
//        sum($"price").cast(DecimalType(10,2)).as("sum_amount"),
//      )
//      .repartition(1)
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_dws.transaction_by_store")
/*
    val avgReview = spark.table("ebs_dwd.review")
      .groupBy("store_id")
      .agg(
        avg("review_score").as("avg_review_score")
      )


    val weekAvg = tran.groupBy("store_id", "tran_year", "tran_month", "tran_month_week")
      .agg(
        size(collect_set("customer_id")).as("week_uq_customer_count")
      )
      .groupBy("store_id")
      .agg(
        avg("week_uq_customer_count").as("week_avg_uq_cnt")
      )



    val weekTop3= tran
      .groupBy("store_id","tran_year","tran_month","tran_month_week")
      .agg(
        sum($"price").cast(DecimalType(10, 2)).as("sum_amount"),
      )
      .withColumn("week_rnk",dense_rank().over(Window.partitionBy(
        "tran_year","tran_month","tran_month_week")
          .orderBy($"sum_amount".desc)
      ))
      .where($"week_rnk"<=3)
      .groupBy($"store_id")
      .agg(count("week_rnk").as("week_top3_count")
      )

    val avgMonthAddCusCnt = tran
      .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy("tran_dt")))
      .where($"rn" === 1)
      .groupBy("store_id", "tran_month")
      .agg(
        count("customer_id").as("new_add_customer_cnt")
      )
      .groupBy("store_id")
      .agg(
        avg("new_add_customer_cnt").as("avg_month_add_customer_cnt")
      )

    val all = tran
      .groupBy("store_id")
      .agg(
        sum($"price").cast(DecimalType(10, 2)).as("sum_amount"),
        size(collect_set("customer_id")).as("total_uq_customer_count")
      )
      .join(weekAvg, "store_id")
      .join(avgMonthAddCusCnt, "store_id")
      .join(avgReview, "store_id")
      .join(weekTop3, "store_id")
      .cache()

    val allMax = all.agg(
      max("sum_amount").as("max_sum_amount"),
      max("total_uq_customer_count").as("max_uq_cus_cnt"),
      max("avg_month_add_customer_cnt").as("max_avg_month_add_customer_cnt"),
      max("week_avg_uq_cnt").as("max_week_avg_uq_cnt"),
      max("avg_review_score").as("max_avg_review_score"),
      max("week_top3_count").as("max_week_top3_count"))


    all.as("A").crossJoin(allMax.as("M"))
      .select($"store_id",
        (
          ($"sum_amount"/$"max_sum_amount"*0.1).cast(DecimalType(5,4))+
            ($"total_uq_customer_count"/$"max_uq_cus_cnt"*0.1).cast(DecimalType(5,4))+
            ($"week_avg_uq_cnt"/$"max_week_avg_uq_cnt"*0.1).cast(DecimalType(5,4))+
            ($"avg_month_add_customer_cnt"/$"max_avg_month_add_customer_cnt"*0.3).cast(DecimalType(5,4))+
            ($"avg_review_score"/$"max_avg_review_score"*0.1).cast(DecimalType(5,4))+
            ($"week_top3_count"/$"max_week_top3_count"*0.3).cast(DecimalType(5,4))
          ).as("sale_factor")
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.store_tran_factor")
*/

//    tran
//      .groupBy("store_id","product")
//      .agg(size(collect_set("customer_id")).as("uq_customer_cnt"))
//      .repartition(1)
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_dws.pop_product_of_store")

    val storeUqCus = tran
      .groupBy("store_id")
      .agg(
        size(collect_set("customer_id")).as("uq_customer_cnt")
      )
/*
    spark.table("ebs_dwd.store")
        .join(storeUqCus,"store_id")
        .select($"store_id",$"employee_number"/$"uq_customer_cnt").as("emp_cus_ratio")
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.store_emp_cus_ration")
*/
/*
    tran
        .groupBy("store_id","tran_year","tran_month")
        .agg(
          sum("price").as("sum_amount")
        )
        .repartition(1)
        .write
        .format("orc")
        .mode(SaveMode.Overwrite)
        .saveAsTable("ebs_dws.store_year_month_sum_amount")


    tran
      .groupBy("store_id","tran_range")
      .agg(
        count("transaction_id").as("range_flow")
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.store_range_flow")
*/
/*
    val storeCustomerAvg = tran
      .groupBy("store_id", "customer_id", "tran_year", "tran_month")
      .agg(
        sum($"price").cast(DecimalType(10, 2)).as("sum_amount"),
        count("transaction_id").as("sum_count")
      )
      .groupBy("store_id", "customer_id")
      .agg(
        avg($"sum_amount").cast(DecimalType(10, 2)) as "month_avg_amount",
        avg($"sum_count").cast(DecimalType(10, 2)) as "month_avg_count"
      )

    val tranTmp = tran
      .withColumn("max_date", max("tran_dt").over(Window.orderBy($"tran_dt".desc)))
      .cache()

    val recent3Month = tranTmp
      .where(datediff($"max_date", $"tran_dt") <= 90)
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount_recent_3_month"),
        count("transaction_id").cast(DecimalType(10, 2)).as("sum_count_recent_3_month")
      )

    val recent3Week = tranTmp
      .where(datediff($"max_date", $"tran_dt") <= 21)
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount_recent_3_week"),
        count("transaction_id").cast(DecimalType(10, 2)).as("sum_count_recent_3_week")
      )

    val storeCusTmp = tran
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount"),
        count("transaction_id").cast(DecimalType(10, 2)).as("sum_count")
      )
      .join(storeCustomerAvg, Seq("store_id", "customer_id"))
      .join(recent3Month, Seq("store_id", "customer_id"))
      .join(recent3Week, Seq("store_id", "customer_id"))
      .cache()

    val storeCusMax = storeCusTmp
      .agg(
        max("sum_amount").as("max_sum_amount"),
        max("sum_count").as("max_sum_count"),
        max("month_avg_amount").as("max_month_avg_amount"),
        max("month_avg_count").as("max_month_avg_count"),
        max("sum_amount_recent_3_week").as("max_sum_amount_recent_3_week"),
        max("sum_count_recent_3_week").as("max_sum_count_recent_3_week"),
        max("sum_amount_recent_3_month").as("max_sum_amount_recent_3_month"),
        max("sum_count_recent_3_month").as("max_sum_count_recent_3_month")
      )

    storeCusTmp.crossJoin(storeCusMax)
      .select($"store_id",$"customer_id",
        (
          ($"sum_amount"/$"max_sum_amount")*0.07+
            ($"sum_count"/$"max_sum_count")*0.07+
            ($"month_avg_amount"/$"max_month_avg_amount")*0.08+
            ($"month_avg_count"/$"max_month_avg_count")*0.08+
            ($"sum_amount_recent_3_month"/$"max_sum_amount_recent_3_month")*0.15+
            ($"sum_count_recent_3_month"/$"max_sum_count_recent_3_month")*0.15+
            ($"sum_amount_recent_3_week"/$"max_sum_amount_recent_3_week")*0.2+
            ($"sum_count_recent_3_week"/$"max_sum_count_recent_3_week")*0.2
        ).cast(DecimalType(5,4)).as("faith_factor")
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_dws.store_faith_factor")
*/


    val review = spark.table("ebs_dwd.review").cache()
//    tran
//        .join(review,Seq("transaction_id"),"left")
//        .agg(
//          (count("review_score")/count("transaction_id")) as "review_coverage"
//        )
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_dws.review_coverage")

    review
        .groupBy("review_score")
      .agg(
        count("*")as "review_distribution"
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ebs_report.review_distribution")

    val cusStoreReview = review
      .where($"review_score" >= 4)
      .as("R")
      .join(tran.as("T"), "transaction_id")
      .groupBy("T.customer_id", "R.store_id")
      .agg(count("transaction_id").as("cus_store_good_cnt"))
      .cache()

//
//    val cusReview = cusStoreReview.groupBy("customer_id")
//      .agg(
//        sum("cus_store_good_cnt").as("cus_good_cnt")
//      )
//        .where($"cus_good_cnt">=3)
//    cusStoreReview
//        .join(cusReview,"customer_id")
//        .select(
//          $"customer_id",$"store_id",
//          ($"cus_store_good_cnt"/$"cus_good_cnt").cast(DecimalType(3,2)).as("cus_store_good_rate")
//        )
//        .where($"cus_store_good_rate">0.66)
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_report.cus_good_review_distribution_of_store")


    val frmScores = spark
      .createDataFrame(Seq(Review(1), Review(2), Review(3), Review(4), Review(5)))
      .select($"review_score".cast(DecimalType(3, 2)))
    val frmStoreSCores = spark.table("ebs_dwd.review")
        .groupBy("store_id","review_score")
        .agg(count("*").as("review_score_cnt"))

    spark.table("ebs_dwd.store")
        .crossJoin(frmScores)
      .as("L")
        .join(frmStoreSCores.as("R"),Seq("store_id","review_score"),"left")
        .withColumn("review_score_cnt",
          when(
            $"review_score_cnt".isNull,0
          ).otherwise($"review_score_cnt")
        )
        .select($"store_name",$"L.review_score",$"review_score_cnt")
        .orderBy($"store_name",$"L.review_score")
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable("ebs_report.review_distribution_of_store")
    spark.stop()
  }

}
