import org.apache.spark.{SparkConf, SparkContext}

object wordCount{
    def main(args: Array[String]): Unit = {
      val logFile = "/logFile"
      val conf = new SparkConf().setAppName("appname").setMaster("yarn")
      val sc = new SparkContext(conf)
      val rdd = sc.textFile(logFile)
      val wc = rdd.flatMap(_.split("")).map((_,1)).reduceByKey(_ + _)
    }
}
