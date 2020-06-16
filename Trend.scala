import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Trend extends App {
val spark = SparkSession.builder().master("yarn").appName("Buz_trend").getOrCreate()
  spark.conf.set("temporaryGcsBucket", "developer-stuff-resource/")

  import spark.implicits._


//*********************public method*******************************


def readSampleData(tableName: String): DataFrame = {
  spark.read
    .format("bigquery")
    .option("project","endless-upgrade-280116")
    .option("table", s"sheenOutputDataSet.${tableName}")
    .load()
}

def writeBQ(df: DataFrame, tableName: String) = {
  df.write.mode(SaveMode.Overwrite)
    .format("bigquery")
    .option("project", "endless-upgrade-280116")
    .option("table", s"taxiBizCount.${tableName}")
    .save()
}

def timeDateTruncForHeat(dataframe: DataFrame): DataFrame = {
  dataframe
    .withColumn("year", substring($"pickup_datetime", 1, 4))
    .withColumn("month", substring($"pickup_datetime", 6, 2))
    .withColumn("day", substring($"pickup_datetime", 9, 2))
    .withColumn("dow", from_unixtime(unix_timestamp($"pickup_datetime", "yyyy-mm-dd"), "EEEEE"))
    .withColumn("hour", substring($"pickup_datetime", 12, 2))
    .withColumn("ten", substring($"pickup_datetime", 15, 1))
    .withColumn("ten_mins", concat($"ten", lit("0")))
}

  def timeDateTruncForCount(dataframe: DataFrame): DataFrame = {
    dataframe
      .withColumn("year", substring($"year_month_day", 1, 4))
      .withColumn("month", substring($"year_month_day", 6, 2))
  }

def fhvSelectPublicColumns(df: DataFrame) = {
  df.select(
    $"location_id"
    , $"pickup_datetime"
    , $"borough"
    , $"zone"
    , $"service_zone")
}

def greenYellowSelPubColumns(df: DataFrame) = {
    df.select($"vendor_id"
      , $"pickup_datetime"
      , $"pickup_longitude"
      , $"pickup_latitude"
      , $"trip_distance"
      , $"passenger_count"
      , $"fare_amount"
      , $"pickup_location_id")
  }
//******************************Raw Data***************************

val taxi_zone_geom = readSampleData("taxi_zone_geom")

val fhv = fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2015_sample"))
  .union(fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2016_sample")))
  .union(fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2017_sample")))


val green = greenYellowSelPubColumns(readSampleData("tlc_green_trips_2014_sample").withColumn("pickup_location_id", lit("missing")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2015_sample").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2016_sample").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2017_sample")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2018_sample").withColumn("pickup_longitude", lit("missing")).withColumn("pickup_latitude", lit("missing"))))



val yellow = greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2015_sample").withColumn("pickup_location_id", lit("missing")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2016_sample").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_2017_sample")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2018_sample").withColumn("pickup_longitude", lit("missing")).withColumn("pickup_latitude", lit("missing"))))

//*****************************select useful columns********************

val fhv_filter = fhv   //对fhv做数据过滤
  .filter($"location_id".isNotNull)
  .filter($"pickup_datetime".isNotNull)
  .filter($"borough".isNotNull)
  .filter($"zone".isNotNull)
  .filter(!$"zone".contains("NV"))
  .filter(!$"zone".contains("NA"))
  .filter($"service_zone".isNotNull)
  .filter(!$"service_zone".contains("N/A"))


val green_filter = green    //2017的没有经纬度信息
  .filter($"vendor_id".isNotNull)
  .filter($"pickup_datetime".isNotNull)
  .filter($"pickup_longitude".isNotNull)
  .filter($"pickup_latitude".isNotNull)
  .filter($"trip_distance".isNotNull)
  .filter($"passenger_count".isNotNull)
  .filter($"fare_amount".isNotNull)
  .filter(!$"fare_amount".startsWith("-"))
  .filter(!($"fare_amount" === "0.0"))
  .filter(!($"trip_distance" === "0.0"))
  .filter(!($"pickup_latitude" === "0.0"))
  .filter(!($"pickup_longitude" === "0.0"))


val yellow_filter = yellow  //2017的没有经纬度信息
  .filter($"vendor_id".isNotNull)
  .filter($"pickup_datetime".isNotNull)
  .filter($"pickup_longitude".isNotNull)
  .filter($"pickup_latitude".isNotNull)
  .filter($"trip_distance".isNotNull)
  .filter($"passenger_count".isNotNull)
  .filter($"fare_amount".isNotNull)
  .filter(!$"fare_amount".startsWith("-"))
  .filter(!($"fare_amount" === "0.0"))
  .filter(!($"trip_distance" === "0.0"))
  .filter(!($"pickup_latitude" === "0.0"))
  .filter(!($"pickup_longitude" === "0.0"))

//过滤的时候要小心，别把有的没有column加入过滤条件
//与taxi的地理信息join起来
val fhv_taxi_zone = fhv_filter.join(taxi_zone_geom, fhv_filter("location_id") === taxi_zone_geom("zone_id")).filter($"pickup_datetime".startsWith("2017"))
  .withColumn("pickup_datetime", regexp_replace(col("pickup_datetime"), "2017", "2018"))
val green_taxi_zone = green_filter.join(taxi_zone_geom, green_filter("pickup_location_id") === taxi_zone_geom("zone_id"))  //注意 这里只有green 18的数据才有zone信息
val yellow_taxi_zone = yellow_filter.join(taxi_zone_geom, yellow_filter("pickup_location_id") === taxi_zone_geom("zone_id")) // 注意 这里只有yellow18的数据才有zone信息
//******************************Data Model Adjust 增加一些日期信息和聚合信息***************************
//******************************为了统计个taxi种类的业务量，我们filter的数据统计,因为这样数据才完整，分析人员的视图***************

val fhv_biz_count = timeDateTruncForCount(fhv_taxi_zone
  .select($"zone_name", $"pickup_datetime")
  .withColumn("year_month_day", substring($"pickup_datetime", 1, 7))
  .select($"year_month_day", $"zone_name")
  .groupBy($"year_month_day", $"zone_name")
  .count()
  .withColumn("taxi_type", lit("FHV")))


  val green_biz_count = timeDateTruncForCount(green_taxi_zone
    .select($"zone_name", $"pickup_datetime")
    .withColumn("year_month_day", substring($"pickup_datetime", 1, 7))
    .select($"year_month_day", $"zone_name")
    .groupBy($"year_month_day", $"zone_name")
    .count()
    .withColumn("taxi_type", lit("Green")))

  val yellow_biz_count = timeDateTruncForCount(yellow_taxi_zone
    .select($"zone_name", $"pickup_datetime")
    .withColumn("year_month_day", substring($"pickup_datetime", 1, 7))
    .select($"year_month_day", $"zone_name")
    .groupBy($"year_month_day", $"zone_name")
    .count()
    .withColumn("taxi_type", lit("Yellow")))

  val taxi_biz_count_total = fhv_biz_count.union(green_biz_count).union(yellow_biz_count)

  writeBQ(taxi_biz_count_total, "taxi_biz_count_total")

//***********************************************司机的视图，这里司机只选取最后一天的数据进行分析，主要对yellow和green的分析****************************************

def filterLastDay(dataframe: DataFrame) = {
  timeDateTruncForHeat(dataframe)
    .filter($"year" === "2016")
    .filter($"month" === "03")
    .filter($"day" === "31")
}
//********************1天的分析采用2016年3月31日的********************
  val green_2016 = green
    .filter($"pickup_datetime".isNotNull)
    .filter($"pickup_longitude".isNotNull)
    .filter($"pickup_latitude".isNotNull)

  val yellow_2016 = yellow
    .filter($"pickup_datetime".isNotNull)
    .filter($"pickup_longitude".isNotNull)
    .filter($"pickup_latitude".isNotNull)

 val green_taxi_oneday_analysis = filterLastDay(green_2016).withColumn("taxi_type", lit("Green"))
 val yellow_taxi_oneday_analysis = filterLastDay(yellow_2016).withColumn("taxi_type", lit("Yellow"))

  val green_yellow_oneday_union = green_taxi_oneday_analysis.union(yellow_taxi_oneday_analysis).drop("ten")

  writeBQ(green_yellow_oneday_union, "green_yellow_oneday_union")

//***************************************************价格预测部分***************************************

 val green_taxi_price_forcast = green_filter.select($"fare_amount".cast(DoubleType), $"trip_distance".cast(DoubleType))
 val yellow_taxi_price_forcast = yellow_filter.select($"fare_amount".cast(DoubleType), $"trip_distance".cast(DoubleType))

 case class fareVStrip(label: Double, features: org.apache.spark.ml.linalg.Vector)
def taxiPriceLinear(df: DataFrame, col1: Column, col2: Column) = {
  import org.apache.spark.ml.linalg.Vectors
  df.select(col1.cast("Double"), col2.cast("Double"))
  .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
    .option("codec", "none")
    .option("header", "false")
    .option("delimiter", ",")
    .save(s"gs://taxi-data-price-csv/fare_vs_trip/")

  val sc = spark.sparkContext
  val fare_trip_csv = sc.textFile(s"gs://taxi-data-price-csv/fare_vs_trip/")
  import spark.implicits._
  val fare_trip = fare_trip_csv.map {line =>
    val splits = line.split(",")
    fareVStrip(splits(0).toDouble, Vectors.dense(splits(1).toDouble))
  }.toDF

  val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
  val lrModel = lr.fit(fare_trip)

  println(s"斜率：${lrModel.coefficients}")
  println(s"截距：${lrModel.intercept}")
  println(s"R squat：${lrModel.summary.r2}")
  println(s"RMSE：${lrModel.summary.rootMeanSquaredError}")
  println(s"MAE：${lrModel.summary.meanAbsoluteError}")
}
  taxiPriceLinear(green_taxi_price_forcast, $"fare_amount", $"trip_distance")
  taxiPriceLinear(yellow_taxi_price_forcast, $"fare_amount", $"trip_distance")

//************************************************************************************************


}
