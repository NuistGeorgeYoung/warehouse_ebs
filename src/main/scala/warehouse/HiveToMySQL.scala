package warehouse

import java.util.Properties
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveToMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hive_to_mysql")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val conf = new Properties()
    val dbName: String = "test_hive_to_mysql"

    conf.setProperty("driver","com.mysql.cj.jdbc.Driver")
    conf.setProperty("url",s"jdbc:mysql://single01:3306/$dbName?createDatabaseIfNotExist=true")
    conf.setProperty("user","root")
    conf.setProperty("password","zzy147258369")

    spark.sql(s"show tables from $dbName")
      .collect()
      .foreach(
        row => {
          val tableName: String = row.getString(1)
          spark.table(s"$dbName.$tableName")
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .jdbc(conf.getProperty("url"),tableName,conf )
        }
      )
  }
}
