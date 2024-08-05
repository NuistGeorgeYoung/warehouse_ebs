package warehouse

import core.MysqlConfigFactory.Getter
import core.{MysqlConfigFactory, SparkFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}

object EbsAdsExport {
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

    val dbName: String = "ebs_report"

    val getter: Getter = MysqlConfigFactory()
      .build()
      .setDriver("com.mysql.cj.jdbc.Driver")
      .setUser("root")
      .setUrl(s"jdbc:mysql://single01:3306/$dbName?createDatabaseIfNotExist=true")
      .setPassword("zzy147258369")
      .finish()

    spark.sql(s"show tables from $dbName")
      .collect()
      .foreach(
        row => {
          val tableName: String = row.getString(1)
          spark.table(s"$dbName.$tableName")
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .jdbc(getter.getUrl, tableName, getter.getConf)
        }
      )


      spark.stop()
  }
}
