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
    .withColumn("location_id", $"location_id".cast("String"))
    .withColumn("pickup_datetime", $"pickup_datetime".cast("String"))
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
      .withColumn("pickup_location_id", $"pickup_location_id".cast("String"))
      .withColumn("pickup_datetime", $"pickup_datetime".cast("String"))
      .withColumn("trip_distance", $"trip_distance".cast("String"))
      .withColumn("fare_amount", $"fare_amount".cast("String"))

}
//******************************Raw Data***************************

val taxi_zone_geom = readSampleData("taxi_zone_geom.csv")

val fhv = fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2015_sample_full"))
  .union(fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2016_sample_full")))
  .union(fhvSelectPublicColumns(readSampleData("tlc_fhv_trips_2017_sample_full")))


val green = greenYellowSelPubColumns(readSampleData("tlc_green_trips_2014_sample_full").withColumn("pickup_location_id", lit("missing")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2015_sample_full").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2016_sample_full").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2017_sample_full")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_green_trips_2018_sample_full").withColumn("pickup_longitude", lit("missing")).withColumn("pickup_latitude", lit("missing"))))



val yellow = greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2015_sample_full").withColumn("pickup_location_id", lit("missing")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2016_sample_full").withColumn("pickup_location_id", lit("missing"))))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2017_sample_full")))
  .union(greenYellowSelPubColumns(readSampleData("tlc_yellow_trips_2018_sample_full").withColumn("pickup_longitude", lit("missing")).withColumn("pickup_latitude", lit("missing"))))

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
  .withColumn("pickup_datetime", regexp_replace(col("pickup_datetime"), "2017", "2018")) //<-----
val green_taxi_zone = green_filter.join(taxi_zone_geom, green_filter("pickup_location_id") === taxi_zone_geom("zone_id"))  //注意 这里只有green 17 18的数据才有zone信息
val yellow_taxi_zone = yellow_filter.join(taxi_zone_geom, yellow_filter("pickup_location_id") === taxi_zone_geom("zone_id")) // 注意 这里只有yellow 17 18的数据才有zone信息
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
    .withColumn("month", $"month".cast("Integer"))
    .withColumn("year_month_day_new", concat($"year_month_day",lit("-00")))

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
    .filter(!$"pickup_longitude".startsWith("0"))
    .filter($"pickup_latitude".isNotNull)

  val yellow_2016 = yellow
    .filter($"pickup_datetime".isNotNull)
    .filter($"pickup_longitude".isNotNull)
    .filter(!$"pickup_longitude".startsWith("0"))
    .filter($"pickup_latitude".isNotNull)

 val green_taxi_oneday_analysis = filterLastDay(green_2016).withColumn("taxi_type", lit("Green"))
 val yellow_taxi_oneday_analysis = filterLastDay(yellow_2016).withColumn("taxi_type", lit("Yellow"))

  val green_yellow_oneday_union = green_taxi_oneday_analysis.union(yellow_taxi_oneday_analysis).drop("ten")
    .withColumn("Count", lit(1))
    .withColumn("City", lit("New York"))
    .drop("trip_distance", "passenger_count", "fare_amount", "pickup_location_id")

  writeBQ(green_yellow_oneday_union, "green_yellow_oneday_union")

//***************************************************价格预测部分，用scala做一元线性回归分析***************************************
case class fareVSvectors(label: Double, features: org.apache.spark.ml.linalg.Vector)
  def taxiPriceLinear(df: DataFrame, col1: String, col2: String) = {
    import org.apache.spark.ml.linalg.Vectors
    df.select(col(col1).cast("Double"), col(col2).cast("Double"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .option("codec", "none")
      .option("header", "false")
      .option("delimiter", ",")
      .save(s"gs://taxi-data-price-csv/${col1}-${col2}/")

    val sc = spark.sparkContext
    val fare_trip_csv = sc.textFile(s"gs://taxi-data-price-csv/${col1}-${col2}/")
    import spark.implicits._
    val fare_trip = fare_trip_csv.map {line =>
      val splits = line.split(",")
      fareVSvectors(splits(0).toDouble, Vectors.dense(splits(1).toDouble))
    }.toDF

    val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val lrModel = lr.fit(fare_trip)

    println(s"斜率：${lrModel.coefficients}")
    println(s"截距：${lrModel.intercept}")
    println(s"R squat：${lrModel.summary.r2}")
    println(s"RMSE：${lrModel.summary.rootMeanSquaredError}")
    println(s"MAE：${lrModel.summary.meanAbsoluteError}")
  }
//***************************************************价格VS行程***************************************************************
 val green_taxi_price_forecast = green_2018.select($"fare_amount".cast(DoubleType), $"trip_distance".cast(DoubleType))
 val yellow_taxi_price_forecast = yellow_filter.select($"fare_amount".cast(DoubleType), $"trip_distance".cast(DoubleType))
  taxiPriceLinear(green_taxi_price_forecast, "fare_amount", "trip_distance")
  taxiPriceLinear(yellow_taxi_price_forecast, "fare_amount", "trip_distance")


//*************************************************价格VS其他******************************************************************
def greenYellowSelForeColumns(df: DataFrame) = {
  df.select($"pickup_datetime".substr(1, 15).alias("pickup_datetime")
    , $"trip_distance"
    , $"fare_amount"
    , $"borough")
    .filter(!$"fare_amount".startsWith("0"))
    .filter(!$"fare_amount".startsWith("-"))
    .filter(!$"trip_distance".startsWith("-"))
    .filter(!$"trip_distance".startsWith("0"))
    .withColumn("month", substring($"pickup_datetime", 6, 2))
    .withColumn("month", $"month".cast("Integer"))
    .withColumn("dow", from_unixtime(unix_timestamp($"pickup_datetime", "yyyy-mm-dd"), "EEEEE"))
}
 val green_2018 =  greenYellowSelForeColumns(green_taxi_zone)
 val yellow_2018 =  greenYellowSelForeColumns(yellow_taxi_zone)

//*************************************************价格VSborough 10分钟水平***************************************************************

def vecPriceFore(df: DataFrame, colName: Column, colValue: String) = {
  df.select($"pickup_datetime", $"fare_amount".cast(DoubleType), $"trip_distance".cast(DoubleType), colName)
    .filter($"borough" === s"${colValue}")
    .groupBy($"pickup_datetime", $"borough")
    .avg("fare_amount", "trip_distance")
}

val green_2018_EWR_fore = vecPriceFore(green_2018,$"borough", "EWR").withColumn("taxi_type", lit("Green"))
val green_2018_Bro_fore = vecPriceFore(green_2018,$"borough", "Bronx").withColumn("taxi_type", lit("Green"))
val green_2018_Que_fore = vecPriceFore(green_2018,$"borough", "Queens").withColumn("taxi_type", lit("Green"))
val green_2018_Broly_fore = vecPriceFore(green_2018,$"borough", "Brooklyn").withColumn("taxi_type", lit("Green"))
val green_2018_Man_fore = vecPriceFore(green_2018,$"borough", "Manhattan").withColumn("taxi_type", lit("Green"))
val green_2018_Sta_fore = vecPriceFore(green_2018,$"borough", "Staten Island").withColumn("taxi_type", lit("Green"))

  val df_green = green_2018_EWR_fore.union(green_2018_Bro_fore).union(green_2018_Que_fore).union(green_2018_Broly_fore).union(green_2018_Man_fore).union(green_2018_Sta_fore)

  val yellow_2018_EWR_fore = vecPriceFore(yellow_2018,$"borough", "EWR").withColumn("taxi_type", lit("Yellow"))
  val yellow_2018_Bro_fore = vecPriceFore(yellow_2018,$"borough", "Bronx").withColumn("taxi_type", lit("Yellow"))
  val yellow_2018_Que_fore = vecPriceFore(yellow_2018,$"borough", "Queens").withColumn("taxi_type", lit("Yellow"))
  val yellow_2018_Broly_fore = vecPriceFore(yellow_2018,$"borough", "Brooklyn").withColumn("taxi_type", lit("Yellow"))
  val yellow_2018_Man_fore = vecPriceFore(yellow_2018,$"borough", "Manhattan").withColumn("taxi_type", lit("Yellow"))
  val yellow_2018_Sta_fore = vecPriceFore(yellow_2018,$"borough", "Staten Island").withColumn("taxi_type", lit("Yellow"))

  val df_yellow = yellow_2018_EWR_fore.union(yellow_2018_Bro_fore).union(yellow_2018_Que_fore).union(yellow_2018_Broly_fore).union(yellow_2018_Man_fore).union(yellow_2018_Sta_fore)
writeBQ(df_green.union(df_yellow)
  .withColumn("fare_amount_avg", $"avg(fare_amount)")
  .withColumn("trip_distance_avg", $"avg(trip_distance)")
  .drop("avg(fare_amount)", "avg(trip_distance)"),"price_predict")
 //************************************************************************************************
}
