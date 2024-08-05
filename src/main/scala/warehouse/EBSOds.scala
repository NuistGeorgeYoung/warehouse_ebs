package warehouse

import core.{MysqlConfigFactory, SparkFactory}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object EBSOds {
  case class Line(json:String)
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

    val mysql: MysqlConfigFactory.Getter = MysqlConfigFactory()
      .build()
      .setDriver("com.mysql.cj.jdbc.Driver")
      .setUrl("jdbc:mysql://single01:3306/ebs?userUnicode=true&charSet=utf8")
      .setUser("root")
      .setPassword("zzy147258369")
      .finish()

    spark.sql("create database if not exists ebs_ods")
    val tables: Array[String] = Array("customer","store")

    import spark.implicits._
    import org.apache.spark.sql.functions._
    tables.foreach(table=>{
      spark.read
        .jdbc(mysql.getUrl,table,mysql.getConf)
        .write
        .mode(SaveMode.Overwrite)
        .format("orc")
        .saveAsTable(s"ebs_ods.$table")
    })
    //行为日志入库
    val sc = spark.sparkContext
    val rddLines = sc.textFile("hdfs://single01:9000/external_ebs/transaction", 4)
      .mapPartitions(_.map(line=>Line(line)))

    import spark.implicits._
    import org.apache.spark.sql.functions._
    spark
      .createDataFrame(rddLines)
        .select(json_tuple($"json","transaction_id","customer_id","store_id","price","product","date","time").as(
          Seq("transaction_id",
            "customer_id",
            "store_id",
            "price",
            "product",
            "date",
            "time"
          )
        )
      )
        .write
        .format("orc")
        .saveAsTable("ebs_ods.transaction")

    val rddLines1 = sc.textFile("hdfs://single01:9000/external_ebs/review", 4)
      .mapPartitions(_.map(line=>Line(line)))

    spark
      .createDataFrame(rddLines1)
      .select(json_tuple($"json","transaction_id","store_id","review_score").as(
        Seq("transaction_id",
          "store_id",
          "review_score"
        )
      )
      )
      .write
      .format("orc")
      .saveAsTable("ebs_ods.review")

    spark.stop()
  }
}
