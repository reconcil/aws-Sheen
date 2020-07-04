import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions.col

object priceRegression extends App {
  /**
   * Calculate the relationship parameter
   */
  val spark = SparkSession.builder().master("yarn").appName("Buz_trend").getOrCreate()
  val taxi_detail = spark.read.parquet("s3://taxidatasamplegeover/oneMonthBiz/*.parquet")
  import spark.implicits._

  def relationShip(df: DataFrame, col1: String, col2: String) = {
    val real = df.select($"${col1}", $"${col2}")
    val rdd_real = real.rdd.map(x=>(x(0).toString.toDouble ,x(1).toString.toDouble))
    val label = rdd_real.map(x=>x._1.toDouble )
    val feature = rdd_real.map(x=>x._2.toDouble )

    val cor_pearson:Double = Statistics.corr(label, feature, "pearson")
    println(cor_pearson)

    val cor_spearman:Double = Statistics.corr(label, feature, "spearman")
    println(cor_spearman)

  }


  relationShip(taxi_detail, "Fare_amount", "Trip_distance")   //0.0779954587735451   0.9160795764969094
  relationShip(taxi_detail, "Fare_amount", "Pickup_latitude") // -0.0075461821579151595  -0.1035135803686105
  relationShip(taxi_detail, "Fare_amount", "Pickup_longitude") // 0.006104604534192626 0.05064398224865199


  case class fareVSvectors(label: Double, features: org.apache.spark.ml.linalg.Vector)
  def taxiPriceLinear(df: DataFrame, col1: String, col2: String, dirName: String) = {
    import org.apache.spark.ml.linalg.Vectors
    df.sample(false, 0.1, 30).select(col(col1).cast("Double"), col(col2).cast("Double"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .option("codec", "none")
      .option("header", "false")
      .option("delimiter", ",")
      .save(s"s3://forecasttaxiprice/${dirName}/${col1}-${col2}/")

    val sc = spark.sparkContext
    val fare_trip_csv = sc.textFile(s"s3://forecasttaxiprice/${dirName}/${col1}-${col2}/")
    import spark.implicits._
    val fare_trip = fare_trip_csv.map {line =>
      val splits = line.split(",")
      fareVSvectors(splits(0).toDouble, Vectors.dense(splits(1).toDouble))
    }.toDF
    fare_trip.show(50)

    val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(fare_trip)

    println(s"斜率：${lrModel.coefficients}")
    println(s"截距：${lrModel.intercept}")
    println(s"R squat：${lrModel.summary.r2}")
    println(s"RMSE：${lrModel.summary.rootMeanSquaredError}")
    println(s"MAE：${lrModel.summary.meanAbsoluteError}")
  }

  val taxiForPriceForecast = spark.read.parquet("s3://taxidatasamplegeover/Total/*.parquet").filter($"taxi_type" === "green" or $"taxi_type" === "yellow").filter($"Pickup_datetime".between("2019-01-01 00:00:00", "2019-12-31 00:00:00")).filter($"Trip_distance" > 1.0).filter($"Fare_amount" > 1.0).select($"Fare_amount" , $"Trip_distance")
  val green_forecast = taxiForPriceForecast.filter($"taxi_type" === "green")
  val yellow_forecast = taxiForPriceForecast.filter($"taxi_type" === "yellow")

  taxiPriceLinear(green_forecast, "Fare_amount", "Trip_distance", "green2019")
  taxiPriceLinear(yellow_forecast, "Fare_amount", "Trip_distance", "yellow2019")
  /**
   * Green 预测结果
   * 斜率：[2.5488253446537765]
   * 截距：6.131967138375027
   * R squat：0.8108542493360649
   * RMSE：5.339834796117596
   * MAE：2.678594766789961
   *
   * Yellow 预测结果
   * 斜率：[2.666984626667545]
   * 截距：5.526849903171023
   * R squat：0.7083570810108307
   * RMSE：7.508360165966041
   * MAE：2.230689943528214
   */


}


/**

root
 |-- City: string (nullable = true)
 |-- taxi_type: string (nullable = true)
 |-- Pickup_longitude: double (nullable = true)
 |-- Pickup_latitude: double (nullable = true)
 |-- Passenger_count: integer (nullable = true)
 |-- Trip_distance: double (nullable = true)
 |-- Fare_amount: double (nullable = true)
 |-- Count: integer (nullable = true)
 |-- Pickup_DataTimeTenMins: timestamp (nullable = true)