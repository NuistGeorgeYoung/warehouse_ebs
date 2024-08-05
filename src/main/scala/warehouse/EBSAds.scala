package warehouse

import core.SparkFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.StringType

object EBSAds {
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

    spark.sql("create database if not exists ebs_report")

    val frmCustomer = spark.table("ebs_dwd.customer").cache()
    val frmStore = spark.table("ebs_dwd.store").cache()
    val frmStoreYearMonth = spark.table("ebs_dws.store_year_month_sum_amount")
    val bcCustomer: Broadcast[DataFrame] = spark.sparkContext.broadcast(frmCustomer)
    val bcStore: Broadcast[DataFrame] = spark.sparkContext.broadcast(frmStore)

//    spark.table("ebs_dws.customer_by_cgc")
//      .where($"gid"===6)
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"agg_cnt".desc)))
//      .where($"rnk"<=3)
//      .select("credit_type","rnk")
//      .orderBy("rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.credit_type_top3")
//
//    spark.table("ebs_dws.job")
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"job_cnt".desc)))
//      .where($"rnk"<=5)
//      .orderBy("rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.job_top5")
////
//    spark.table("ebs_dws.tran_dim_date")
//      .where($"gid"===15)
//      .select("tran_year","tran_quarter","sum_amount")
//      .orderBy("tran_year","tran_quarter")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.year_quarter_sum_amount")
//
//    spark.table("ebs_dws.tran_dim_date")
//      .where($"gid"===0)
//      .groupBy("tran_range")
//      .agg(sum("sum_amount").as("sum_amount"))
//      .orderBy("tran_range")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.range_sum_amount")
//
//
//    spark
//        .table("ebs_dws.transaction_by_ymc")
//      .where($"gid"===6)
//      .select("customer_id","sum_amount")
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"sum_amount".desc)))
//      .where($"rnk"<10)
//      .join(bcCustomer.value,"customer_id")
//      .select("customer_id","sum_amount","rnk")
//      .orderBy($"rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.range_sum_amount")
//
//    spark
//      .table("ebs_dws.uq_customer_count_by_qw")
//      .where($"gid"===3)
//      .select(
//        concat_ws("_",$"tran_year".cast(StringType),$"tran_quarter".cast(StringType)).as("year_quarter"),
//        $"uq_customer_count"
//      )
//      .orderBy("year_quarter")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.year_quarter")
//
//    spark
//        .table("ebs_dws.tran_product")
//        .withColumn("rnk",dense_rank().over(Window.orderBy($"sum_amount".desc)))
//        .select("product","sum_amount","rnk")
//        .orderBy("rnk")
//        .repartition(1)
//        .write
//        .mode(SaveMode.Overwrite)
//        .format("orc")
//        .saveAsTable("ebs_report.pro_top5_sum_amount")
//
//    spark
//      .table("ebs_dws.store_tran_factor")
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"sale_factor".desc)))
//      .where($"rnk"<=3)
//      .join(bcStore.value,"store_id")
//      .select("store_name","sale_factor","rnk")
//      .orderBy("rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.most_adorable_3_store")
//
//
//
//    spark
//        .table("ebs_dws.dim_date")
//        .groupBy("tran_year","tran_month")
//        .count()
//        .crossJoin(bcStore.value)
//        .join(frmStoreYearMonth,Seq("store_id","tran_year","tran_month"),"left")
//        .withColumn("lag_sum_amount",
//          lag("sum_amount",1)
//            .over(Window.partitionBy("store_id").orderBy("tran_month"))
//        )
//        .where($"tran_month"=!=1)
////      .join(bcStore.value,"store_id")
//        .select($"store_name",
//          concat($"tran_month".cast(StringType),lit("/"),($"tran_month"-1).cast(StringType))as("chain_name")
//            ,($"sum_amount"-$"lag_sum_amount").as("chain_value")
//
//        )
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_report.store_2018_month_chain")
//
//
//    spark
//        .table("ebs_dws.transaction_by_country")
//        .select("country","sum_amount")
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_report.country_dis_by_sum_amount")
//    spark.table("ebs_dwd.customer")
//        .groupBy("country")
//        .agg(
//          count("*").as("country_cnt")
//        )
//        .agg(
//          sum("country_cnt").as("all_cnt"),
//          sum(when($"country"==="China",$"country_cnt").otherwise(0)).as("china_cnt"),
//          sum(when($"country"==="Canada",$"country_cnt").otherwise(0)).as("canada_cnt"),
//          sum(when($"country"==="United States",$"country_cnt").otherwise(0)).as("usa_cnt")
//        )
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_report.customer_singles")

//spark.table("ebs_dws.store_tran_factor")
//    .withColumn("rnk",dense_rank().over(Window.orderBy($"sale_factor".desc)))
//    .where($"rnk"<=3)
//    .join(bcStore.value,"store_id")
//    .select("store_name","sale_factor","rnk")
//    .agg(
//        sum(when($"store_name" ==="Lablaws",$"sale_factor").otherwise(0)).as("Lablaws_sale_factor"),
//      sum(when($"store_name" ==="Walmart",$"sale_factor").otherwise(0)).as("walmart_sale_factor"),
//        sum(when($"store_name" ==="FoodMart",$"sale_factor").otherwise(0)).as("foodmart_sale_factor")
//    )
//  .repartition(1)
//  .write
//  .format("orc")
//  .mode(SaveMode.Overwrite)
//  .saveAsTable("ebs_report.store_singles")

//    spark
//        .table("ebs_dws.transaction_by_country")
//        .withColumn("country",
//          when($"country"==="China","domestic")
//            .otherwise("foreign")
//        )
//        .groupBy("country")
//        .agg(
//          sum("sum_amount").as("sum_amount"),
//          sum("tran_count").as("tran_count"),
//          sum("uq_customer_count").as("uq_customer_count"),
//        )
//        .agg(
//          sum("sum_amount").as("all_amount"),
//          sum("tran_count").as("all_count"),
//          sum(when($"country"==="domestic",$"sum_amount").otherwise(0)).as("domestic_amount"),
//          sum(when($"country"==="foreign",$"sum_amount").otherwise(0)).as("foreign_amount"),
//
//          sum(when($"country"==="domestic",$"tran_count").otherwise(0)).as("domestic_count"),
//          sum(when($"country"==="foreign",$"tran_count").otherwise(0)).as("foreign_count"),
//
//          sum(when($"country"==="foreign",$"uq_customer_count").otherwise(0)).as("foreign_uq_customer_count"),
//          sum(when($"country"==="domestic",$"uq_customer_count").otherwise(0)).as("domestic_uq_customer_count"),
//        )
//      .repartition(1)
//      .write
//      .format("orc")
//      .mode(SaveMode.Overwrite)
//      .saveAsTable("ebs_report.transaction_singles_one")
//
//    spark
//        .table("ebs_dws.customer_by_cgc")
//        .where($"gid"===1)
//        .groupBy("gender")
//        .agg(
//          sum("agg_cnt").as("customer_count"),
//        )
//        .agg(
//          sum(when($"gender"==="Male",$"customer_count").otherwise(0)).as("male_customer_count"),
//          sum(when($"gender"==="Female",$"customer_count").otherwise(0)).as("female_customer_count")
//        )
//        .repartition(1)
//        .write
//        .format("orc")
//        .mode(SaveMode.Overwrite)
//        .saveAsTable("ebs_report.transaction_singles_two")


//    spark.table("ebs_dws.tran_dim_date")
//      .where($"gid"===15)
//      .select("tran_year","tran_quarter","sum_amount")
//      .orderBy("tran_year","tran_quarter")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.sum_amount_by_year_quarter")
//
//    spark.table("ebs_dws.tran_dim_date")
//      .where($"gid"===0)
//      .groupBy("tran_range")
//      .agg(sum("sum_amount").as("sum_amount"))
//      .orderBy("tran_range")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.sum_amount_by_range")
//
//    val tranYmc = spark.table("ebs_dws.transaction_by_ymc").cache()
//    tranYmc
//      .where($"gid"===6)
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"sum_amount".desc)))
//      .where($"rnk"<=10)
//      .join(bcCustomer.value,"customer_id")
//      .select("customer_id","sum_amount","rnk")
//      .orderBy("rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.customer_top10_by_tran_amount")
//
//    tranYmc
//      .where($"gid"===6)
//      .withColumn("rnk",dense_rank().over(Window.orderBy($"tran_count".desc)))
//      .where($"rnk"<=10)
//      .join(bcCustomer.value,"customer_id")
//      .select("customer_id","tran_count","rnk")
//      .orderBy("rnk")
//      .repartition(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("orc")
//      .saveAsTable("ebs_report.customer_top10_by_tran_count")
    spark.table("ebs_dws.uq_customer_count_by_qw")
      .where($"gid"===3)
      .select(
        concat_ws("_",$"tran_year".cast(StringType),$"tran_quarter".cast(StringType)).as("year_quarter"),
        $"uq_customer_count"
      )
      .orderBy("year_quarter")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ebs_report.uq_customer_count_by_year_quarter")
    spark.stop()

  }
}
