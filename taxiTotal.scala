import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object taxiTotal extends App {
  val spark = SparkSession.builder().master("yarn").appName("Buz_trend").getOrCreate()

//This time will using a total table store all type of taxi
//union for difference schema dataframe*******************************************************************
  def unionPro(DFList: List[DataFrame], spark: org.apache.spark.sql.SparkSession): DataFrame = {

    /**
     * This Function Accepts DataFrame with same or Different Schema/Column Order.With some or none common columns
     * Creates a Unioned DataFrame
     */

    import spark.implicits._

    val MasterColList: Array[String] = DFList.map(_.columns).reduce((x, y) => (x.union(y))).distinct

    def unionExpr(myCols: Seq[String], allCols: Seq[String]): Seq[org.apache.spark.sql.Column] = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _                       => lit(null).as(x)
      })
    }

    // Create EmptyDF , ignoring different Datatype in StructField and treating them same based on Name ignoring cases

    val masterSchema = StructType(DFList.map(_.schema.fields).reduce((x, y) => (x.union(y))).groupBy(_.name.toUpperCase).map(_._2.head).toArray)

    val masterEmptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], masterSchema).select(MasterColList.head, MasterColList.tail: _*)

    DFList.map(df => df.select(unionExpr(df.columns, MasterColList): _*)).foldLeft(masterEmptyDF)((x, y) => x.union(y))

  }

  /**
   * format the schema for all data
   */

  def formatSchema(df: DataFrame): DataFrame ={
    import spark.implicits._

    df.select(
      $"Pickup_date"
      , $"Pickup_longitude"
      , $"Pickup_latitude"
      , $"Passenger_count"
      , $"Trip_distance"
      , $"Fare_amount"
      , $"City"
      , $"taxi_type"
      , $"locationID"
    )
  }


  /**
   * filter unreasonable data
   */

  def filterUnreason(df: DataFrame): DataFrame = {
    import spark.implicits._
    df.filter(!($"Passenger_count" === "0")).filter(!($"Fare_amount".startsWith("-"))).filter(!($"Pickup_longitude" === "0")).filter(!($"Pickup_latitude" === "0"))
  }

  /**
   * add the taxi_zone_geom information
   */

  //val taxi_gemo = spark.read.format("com.databricks.spark.csv").option("header", "true").load("./taxi_zone_geom.csv")

  val taxi_gemo = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://taxigemozone")




  import spark.implicits._

//***********************fhv****************************


  val fhv2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2015*").filter($"Pickup_date".contains("2015"))
  val fhv2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2016*").filter($"Pickup_date".contains("2016"))
  val fhv2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2017*").filter($"Pickup_DateTime".contains("2017"))
  val fhv2017_new = fhv2017.withColumn("Pickup_date", $"Pickup_DateTime").withColumn("locationID", $"PUlocationID").drop("Pickup_DateTime", "PUlocationID")
  val fhv_all = unionPro(List(fhv2015, fhv2016, fhv2017_new), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("fhv")).select($"Pickup_date", $"locationID", $"City", $"taxi_type").filter($"locationID".isNotNull)


//************************fhvhv************************

  val fhvhv = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhvhv_tripdata_2019*").withColumn("City", lit("New York")).withColumn("taxi_type", lit("fhvhv"))
  val fhvhv_all = fhvhv.withColumn("Pickup_date", $"pickup_datetime").withColumn("locationID", $"PULocationID").select($"Pickup_date", $"locationID", $"City", $"taxi_type").filter($"locationID".isNotNull).filter($"Pickup_date".startsWith("2019"))

  val df = fhv_all.union(fhvhv_all)
  import spark.implicits._


//************************green************************

  def greenSchema(df: DataFrame): DataFrame = {
    df.withColumn("Pickup_date", $"lpep_pickup_datetime").select($"Pickup_date", $"Pickup_longitude", $"Pickup_latitude", $"Passenger_count", $"Trip_distance", $"Fare_amount", $"Trip_type", $"PULocationID", $"DOLocationID", $"City", $"taxi_type")

  }
  val green2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2013*").filter($"lpep_pickup_datetime".contains("2013"))
  val green2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2014*").filter($"lpep_pickup_datetime".contains("2014"))
  val green2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2015*").filter($"lpep_pickup_datetime".contains("2015"))
  val green2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2016*").filter($"lpep_pickup_datetime".contains("2016"))
  val green2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2017*").filter($"lpep_pickup_datetime".contains("2017")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance")
  val green2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2018*").filter($"lpep_pickup_datetime".contains("2018")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance")
  val green2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2019*").filter($"lpep_pickup_datetime".contains("2019")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance")

  val green_all = greenSchema(unionPro(List(green2013, green2014, green2015, green2016, green2017, green2018, green2019), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("green"))).withColumn("locationID", $"PULocationID").drop("PULocationID").drop("Trip_type").drop("DOLocationID")


//************************yellow**************************


  def yellowSchema(df: DataFrame): DataFrame = {
    df.select($"Pickup_date", $"Pickup_longitude",$"Pickup_latitude", $"Passenger_count", $"Trip_distance", $"Fare_amount",  $"PULocationID", $"DOLocationID", $"City", $"taxi_type")
  }


  val yellow2009 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2009*").withColumn("Pickup_date", $"Trip_Pickup_DateTime").withColumn("Trip_distance", $"Trip_Distance").withColumn("Pickup_longitude", $"Start_Lon").withColumn("Pickup_latitude", $"Start_Lat").withColumn("Fare_amount", $"Fare_Amt").withColumn("Passenger_count", $"Passenger_Count").filter($"Trip_Pickup_DateTime".contains("2009"))
  val yellow2010 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2010*").withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").filter($"pickup_datetime".contains("2010"))
  val yellow2011 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2011*").filter($"pickup_datetime".contains("2011")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount")
  val yellow2012 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2012*").filter($"pickup_datetime".contains("2012")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount")
  val yellow2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2013*").filter($"pickup_datetime".contains("2013")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount")
  val yellow2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2014*").filter($" pickup_datetime".contains("2014")).withColumn("Pickup_date", $" pickup_datetime").withColumn("Passenger_count", $" passenger_count").withColumn("Trip_distance", $" trip_distance").withColumn("Pickup_longitude", $" pickup_longitude").withColumn("Pickup_latitude", $" pickup_latitude").withColumn("Fare_amount", $" fare_amount")
  val yellow2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2015*").filter($"tpep_pickup_datetime".contains("2015")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount")
  val yellow2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2016*").filter($"tpep_pickup_datetime".contains("2016")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount")
  val yellow2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2017*").filter($"tpep_pickup_datetime".contains("2017")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount")
  val yellow2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2018*").filter($"tpep_pickup_datetime".contains("2018")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount")
  val yellow2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2019*").filter($"tpep_pickup_datetime".contains("2019")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount")

  val yellow_all = yellowSchema(unionPro(List(yellow2009, yellow2010, yellow2011, yellow2012, yellow2013, yellow2014, yellow2015, yellow2016, yellow2017, yellow2018, yellow2019), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("yellow"))).withColumn("locationID", $"PULocationID").drop("PULocationID").drop("DOLocationID")



  val df1 = green_all.union(yellow_all)
//  val df1_format = formatSchema(df1)

  val taxi_total_records = unionPro(List(df, df1), spark)

  // <---
//  val t_t_r = unionPro(List(df1_format, df), spark)

import spark.implicits._
  val taxi_total_geo = taxi_total_records.join(taxi_gemo, taxi_total_records("locationID") === taxi_gemo("zone_id"), "left_outer").drop("zone_id").withColumn("Pickup_datetime", unix_timestamp($"Pickup_date", "yyyy-MM-dd HH:mm:ss").cast(TimestampType)).drop("Pickup_date").withColumn("Count", lit("1")).select($"Pickup_datetime", $"locationID".cast(IntegerType), $"City", $"taxi_type", $"Pickup_longitude".cast(DoubleType), $"Pickup_latitude".cast(DoubleType), $"Passenger_count".cast(IntegerType), $"Trip_distance".cast(DoubleType), $"Fare_amount".cast(DoubleType), $"borough", $"Count".cast(IntegerType))




  taxi_total_geo.write.mode(SaveMode.Overwrite).format("parquet").save("s3://taxidatasamplegeover/Total/") //total

  //using for all biz count
  import org.apache.spark.sql.functions._

  val bizCount = spark.read.parquet("s3://taxidatasamplegeover/Total/*.parquet")
  val format_totalbizCount = bizCount.select($"Pickup_datetime", date_format($"Pickup_datetime", "yyyy-MM-dd").alias("Pickup_dateday"), $"borough", $"taxi_type").drop($"Pickup_datetime")
  val grouped_biz_count = format_totalbizCount.groupBy($"Pickup_dateday", $"borough", $"taxi_type").count()
  val withdetail_biz_count = grouped_biz_count.withColumn("Pickup_DateDay", unix_timestamp($"Pickup_dateday", "yyyy-MM-dd").cast(TimestampType)).withColumn("Month", month($"Pickup_DateDay")).withColumn("Day", dayofmonth($"Pickup_DateDay")).withColumn("DOW", from_unixtime(unix_timestamp($"Pickup_DateDay", "yyyy-mm-dd"), "EEEEE")).drop($"Pickup_datetime")
  val trunctime_biz_count = withdetail_biz_count.withColumn("Count", $"count".cast(IntegerType))
  trunctime_biz_count.repartition(1).write.mode(SaveMode.Overwrite).format("parquet").save("s3://taxidatasamplegeover/totalBiz/")




  //using for last month with lat and lon data to make the hitmap and price predict

  val taxi_with_latlon = bizCount.filter($"Pickup_datetime".between("2015-12-00 00:00:00", "2016-01-01 00:00:00")).filter($"taxi_type" === "green" or $"taxi_type" === "yellow").withColumn("Pickup_DataTimeTenMins", substring(date_format($"Pickup_datetime", "yyyy-MM-dd HH:mm"), 1, 15)).withColumn("Pickup_DataTimeTenMins", concat($"Pickup_DataTimeTenMins", lit("0"))).drop("Pickup_datetime", "locationID", "borough")
  val taxi_with_latlon_time = taxi_with_latlon.withColumn("Pickup_DataTimeTenMins", unix_timestamp($"Pickup_DataTimeTenMins", "yyyy-MM-dd HH:mm").cast(TimestampType)).filter($"Pickup_longitude" < 0).filter($"Pickup_latitude" > 0).filter($"Passenger_count" > 0).filter($"Fare_amount" > 0).filter($"Passenger_count" > 0)
  taxi_with_latlon_time.repartition(1).write.mode(SaveMode.Overwrite).format("parquet").save("s3://taxidatasamplegeover/oneMonthBiz/")



}

