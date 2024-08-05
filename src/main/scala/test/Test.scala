package test

import core.{MysqlConfigFactory, SparkFactory}
import core.SparkFactory.Builder
import org.apache.spark.sql.{SaveMode, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
//    def baseConfig(appName:String,master:String="local[*]",deployMode:String="client",eventLogEnabled:Boolean=false):Builder
//    def optimizeDriver(memoryGB:Int=2,coreNum:Int=2,maxRstGB:Int=1,driverHost:String="localhost"):Builder
//    def optimizeExecutor(memoryGB:Int=2,coreNum:Int=2):Builder
//    def optimizeLimit(maxCores:Int=6,maxTaskFailure:Int=3,maxLocalWaitS:Int=40):Builder
//    def optimizeSerializer(serde:String="org.apache.spark.serializer.JavaSerializer",clas:Array[Class[_]]=null):Builder
//    def optimizeNetAbout(netTimeoutS:Int=180,schedulerMode:String="FAIR"):Builder
//    def optimizeDynamicAllocation(dynamicEnabled:Boolean=false,initialExecutors:Int=6,minExecutors:Int=0,maxExecutors:Int=10):Builder
//    def optimizeShuffle(parallelism:Int=4,shuffleCompressEnabled:Boolean=false,maxSizeMB:Int=128,shuffleServiceEnabled:Boolean=false):Builder
//    def optimizeSpeculation(speculationEnabled:Boolean=false,intervalS:Int=15,quantile:Float=0.75f):Builder
//    def warehouseDir(hdfsUrl:String):Builder
//    def end():SparkSession

    val spark = SparkFactory()
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
spark.read.table("ebs_ods.store").where($"store_id"isNull)

//    spark.table("dblab.user_action").orderBy("visit_date").show(20)
val conf = MysqlConfigFactory()
  .build
  .setDriver("com.mysql.cj.jdbc.Driver")
  .setUrl("jdbc:mysql://single01:3306/ebs")
  .setUser("root")
  .setPassword("zzy147258369")
  .finish()
//
//    spark.read.jdbc(conf.getUrl,"Movies",conf.getConf).show()

//  spark.table("rsda_ods.store")
//      .write
//      .mode(SaveMode.Overwrite)
//      .jdbc(conf.getUrl,"store",conf.getConf)


//    spark.stop()
  }
}
