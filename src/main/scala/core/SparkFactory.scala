package core
import core.SparkFactory.Builder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import core.Validator.check
class SparkFactory {

  def build(): Builder ={
    new Builder {
      val conf:SparkConf = new SparkConf()
      /**
       * 单向配置
       * @param item 配置项名称
       * @param value 配置项值
       * @param regexValue 配置项值正则规则
       * @return
       */
      private def set(item:String,value: String,regexValue:String=null)={
        check("name_of_config_item",item,"^spark\\..*")
        check(item,value,regexValue)
        conf.set(item,value)
      }

      private def setBaseAppName(appName:String)={
        set("spark.app.name",appName,"^\\w+$")
      }
      private def setBaseMaster(master:String)={
        set("spark.master",master,"local(\\[(\\*|\\[1-9][0-9]*)])?|spark://([a-z]\\w+|\\d{1,3}(\\.\\d{1,3}{3})):\\d{4,5}|yarn")
      }

      private def setBaseDeployMode(deployMode:String)={
        set("spark.submit.deployMode",deployMode,"client|cluster")
      }
      private def setBaseEventLogEnabled(eventLogEnable:Boolean)={
        set("spark.eventLog.enabled",s"$eventLogEnable")
      }

      def baseConfig(appName:String,master:String="local[*]",deployMode:String="client",eventLogEnabled:Boolean=false):Builder={
        setBaseAppName(appName)
        setBaseMaster(master)
        setBaseDeployMode(deployMode)
        setBaseEventLogEnabled(eventLogEnabled)
        this
      }

      private def setDriverMemory(memoryGB:Int)={
        set("spark.driver.memory",s"${memoryGB}g","[1-9]\\d*g")
      }
      private def setDriverCoreNum(coreNum:Int)={
        set("spark.driver.cores",s"$coreNum","[1-9]\\d*")
      }
      private def setDriverMaxResultGB(maxRstGB:Int)={
        set("spark.driver.maxResultSize",s"${maxRstGB}g","[1-9]\\d*g")
      }
      private def setDriverHost(driverHost:String)={
        set("spark.driver.host",driverHost,"[1-9]\\d*")
      }
      def optimizeDriver(memoryGB:Int=2,coreNum:Int=2,maxRstGB:Int=1,driverHost:String="localhost"):Builder={
        setDriverMemory(memoryGB)
        setDriverCoreNum(coreNum)
        setDriverMaxResultGB(maxRstGB)
        if(conf.get("spark.master").startsWith("spark://")){setDriverHost(driverHost)}
        this
      }

      private def setExecutorMemory(memoryGB:Int)={
        set("spark.executor.memory",s"${memoryGB}g","[1-9]\\d*g")
      }
      private def setExecutorCoreNum(coreNum:Int)={
        set("spark.executor.cores",s"$coreNum","[1-9]\\d*")
      }
      def optimizeExecutor(memoryGB:Int=1,coreNum:Int=1):Builder={
        setExecutorMemory(memoryGB)
        if(conf.get("spark.master").startsWith("spark://")){
          setExecutorCoreNum(coreNum)
        }
        this
      }

      private def setLimitmaxCores(maxCores:Int)={
        set("spark.cores.max",s"$maxCores","[1-9]\\d*")
      }
      private def setLimitMaxTaskFailure(maxTaskFailure:Int)={
        set("spark.task.maxFailures",s"$maxTaskFailure","[1-9]\\d*")
      }
      private def setLimitMaxLocalWaitS(maxLocalWaitS:Int)={
        set("spark.locality.wait",s"${maxLocalWaitS}s","[1-9]\\d*s")
      }
      def optimizeLimit(maxCores:Int=4,maxTaskFailure:Int=5,maxLocalWaitS:Int=30):Builder={
        if(conf.get("spark.master").startsWith("spark://")){
          setLimitmaxCores(maxCores)
        }

        /**
         * 单个任务允许失败的最大次数。超出会杀死本次job，重试。最大的重试次数为1
         * 不同任务之间的失败总次数，不会导致作业失败，此值应大于等于1
         */
        setLimitMaxTaskFailure(maxTaskFailure)
        setLimitMaxLocalWaitS(maxLocalWaitS)
        /**
         * 数据本地化读取加载的最大等待时间
         * 大任务:建议适当
         */
        this
      }


      def optimizeSerializer(serde:String="org.apache.spark.serializer.JavaSerializer",clas:Array[Class[_]]=null):Builder={

        /**
         * 将需要通过网络发送或快速缓存的对象序列化的工具类
         * 模式为Java序列化 org.apache.spark.serializer.JavaSerializer
         * 为了提速推荐设置为 org.apache.spark.serializer.KryoSerializer
         * 若采用KryoSerializer序列化方式，需要将所有自定义实体类(样例类)注册到配置中心
         */
        set("spark.serializer",serde,"([a-z]+\\.)+[A-Z]\\w*")
        if (serde.equals("org.apache.spark.serializer.KryoSerializer")) {
          conf.registerKryoClasses(clas)
        }
        this
      }

      private def setNetTimeout(netTimeOut:Int)={
        set("spark.network.timeout",s"${netTimeOut}s","[1-9]\\d*s")
      }
      private def setSchedulerMods(schedulerMods:String)={
        set("spark.scheduler.mode",schedulerMods,"FAIR|FIFO")
      }
      def optimizeNetAbout(netTimeoutS:Int=100,schedulerMods:String="FIFO"):Builder={
        /**
         * 所有和网络交互相关的超时阈值
         */
        setNetTimeout(netTimeoutS)

        /**
         * 多人工作模式下：建议设置为FAIR
         */
        setSchedulerMods(schedulerMods)
        this
      }

      private def setDynamicEnabled(dynamicEnabled:Boolean)={
        set("spark.dynamicAllocation.enabled",s"$dynamicEnabled")
      }
      private def setDynamicInitialExecutors(initialExecutors:Int)={
        set("spark.dynamicAllocation.initialExecutors",s"$initialExecutors","[1-9]\\d*")
      }
      private def setDynamicMinExecutors(minExecutors:Int)={
        set("spark.dynamicAllocation.minExecutors",s"$minExecutors","[1-9]\\d*")
      }
      private def setDynamicMaxExecutors(maxExecutors:Int)={
        set("spark.dynamicAllocation.maxExecutors",s"$maxExecutors","[1-9]\\d*")
      }
      def optimizeDynamicAllocation(dynamicEnabled:Boolean=false,initialExecutors:Int=3,minExecutors:Int=0,maxExecutors:Int=6):Builder={


        if (dynamicEnabled){
          setDynamicEnabled(true)
          setDynamicInitialExecutors(initialExecutors)
          setDynamicMinExecutors(minExecutors)
          setDynamicMaxExecutors(maxExecutors)
        }
        this
      }


      private def setShuffleParallelism(parallelism:Int)={
        set("spark.default.parallelism",s"$parallelism","[1-9]\\d*")
      }
      private def setShuffleCompressEnabled(shuffleCompressEnabled:Boolean)={
        set("spark.shuffle.compress",s"$shuffleCompressEnabled")
      }
      private def setShuffleMaxPerReducer(maxSizeMB:Int)={
        set("spark.reducer.maxSizeInFlight",s"${maxSizeMB}m","[1-9]\\d*m")
      }
      private def setShuffleServiceEnabled(shuffleServiceEnabled:Boolean)={
        set("spark.shuffle.service.enabled",s"$shuffleServiceEnabled")
      }
      def optimizeShuffle(parallelism: Int=3,shuffleCompressEnabled:Boolean=false,maxSizeMB:Int=48,shuffleServiceEnabled:Boolean=false):Builder={
        /**
         * 如果用户没有指定分区数 (numpar/partitoner)，则采用该值作为默认的分区数
         */

        setShuffleParallelism(3)
        /**
         * Shuffle 过程中 Map 端的输出数据是否压缩，建议生成过程中，数据规模较大时开启
         */
        setShuffleCompressEnabled(shuffleCompressEnabled)
        /**
         * 设置Reducer 端缓冲区大小，生成环境中，服务器内存较大时，可以适当调大
         */
        setShuffleMaxPerReducer(maxSizeMB)

        /**
         * 开启一个独立的外部的服务，专门处理 Executor 产生的数据
         */
        setShuffleServiceEnabled(shuffleServiceEnabled)

        this
      }

      private def setSpeculationEnabled(speculationEnabled:Boolean)={
        set("spark.speculation",s"$speculationEnabled")
      }
      private def setShuffleIntervalS(interval:Int)={
        set("spark.speculation.interval",s"${interval}s","[1-9]\\d*s")
      }
      private def setShuffleQuantile(quantile:Float)={
        set("spark.speculation.quantile",s"$quantile","0?\\.\\d+")
      }
      def optimizeSpeculation(speculationEnabled:Boolean=false,interval:Int=5,quantile:Float=0.75F):Builder={
        if (speculationEnabled) {
          /**
           * 是否开启推测执行服务，将各阶段中执行慢的任务(Task)重启
           */
          setSpeculationEnabled(true)

          /**
           * 推荐频次
           */
          setShuffleIntervalS(interval)

          /**
           * 开启推测执行前，任务完成的比例
           */
          setShuffleQuantile(quantile)
        }
        this
      }


      def warehouseDir(hdfs:String):Builder={
        set("spark.sql.warehouse.dir",hdfs)

        this
      }
      def end():SparkSession={
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
      private def setAdaptiveEnabled(adaptiveEnabled:Boolean)={
        set("spark.sql.adaptive.enabled",s"$adaptiveEnabled")
      }
      private def setAutoBroadCastThreshold(thresholdMB:Int)={
        set("spark.sql.autoBroadcastJoinThreshold",s"${thresholdMB}MB","[1-9]\\d*MB")
      }
      private def setCBOEnabled(enabled:Boolean)={
        set("spark.sql.cbo.enabled",s"$enabled")
      }
      def optimizeRuntime(adaptiveEnabled:Boolean=false, threshold:Int=10, cboEnabled:Boolean)={
        /**
         * spark.sql.adaptive.skewJoin.enabled 默认为 true
         * 但只有当 spark.sql.adaptive.enabled(默认为 false) 设置为 true 时生效
         */
        setAdaptiveEnabled(adaptiveEnabled)
        setAutoBroadCastThreshold(threshold)
        setCBOEnabled(cboEnabled)
        this
      }

    }
  }
}
object SparkFactory{
  def apply(): SparkFactory = new SparkFactory()
  trait Builder{
    def baseConfig(appName:String,master:String="local[*]",deployMode:String="client",eventLogEnabled:Boolean=false):Builder
    def optimizeDriver(memoryGB:Int=2,coreNum:Int=2,maxRstGB:Int=1,driverHost:String="localhost"):Builder
    def optimizeExecutor(memoryGB:Int=1,coreNum:Int=1):Builder
    def optimizeLimit(maxCores:Int=4,maxTaskFailure:Int=5,maxLocalWaitS:Int=30):Builder
    def optimizeSerializer(serde:String="org.apache.spark.serializer.JavaSerializer",clas:Array[Class[_]]=null):Builder
    def optimizeNetAbout(netTimeoutS:Int=100,schedulerMods:String="FAIR"):Builder
    def optimizeDynamicAllocation(dynamicEnabled:Boolean=false,initialExecutors:Int=3,minExecutors:Int=0,maxExecutors:Int=6):Builder
    def optimizeShuffle(parallelism: Int=3,shuffleCompressEnabled:Boolean=false,maxSizeMB:Int=128,shuffleServiceEnabled:Boolean=true):Builder
    def optimizeSpeculation(speculationEnabled:Boolean=false,interval:Int=15,quantile:Float=0.75F):Builder
    def warehouseDir(hdfs:String):Builder
    def end():SparkSession

  }
}