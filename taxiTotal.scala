import org.apache.spark.sql.types.StructType
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

  val taxi_gemo = spark.read.format("com.databricks.spark.csv").option("header", "true").load("./taxi_zone_geom.csv")

  /*
  val taxi_gemo = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://taxigemozone")
  +-------+-------+
|zone_id|borough|
+-------+-------+
|      1|    EWR|
|      3|  Bronx|
|     18|  Bronx|
|     20|  Bronx|
|     31|  Bronx|
|     32|  Bronx|
|     46|  Bronx|
|     47|  Bronx|
|     51|  Bronx|
|     58|  Bronx|
|     59|  Bronx|
|     60|  Bronx|
|     69|  Bronx|
   */


  import spark.implicits._

//***********************fhv****************************
  /**
   *
   * fhv_all schema
  root
 |-- Pickup_date: string (nullable = true)
 |-- locationID: string (nullable = true)
 |-- City: string (nullable = false)
 |-- taxi_type: string (nullable = false)
   fhv_all count = 3896638
   fhv_all locationID notNull = 2889126

   */

  val fhv2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2015*").filter($"Pickup_date".contains("2015")).sample(false, 0.001, 30)
  val fhv2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2016*").filter($"Pickup_date".contains("2016")).sample(false, 0.001, 30)
  val fhv2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2017*").filter($"Pickup_DateTime".contains("2017")).sample(false, 0.001, 30)
  val fhv2017_new = fhv2017.withColumn("Pickup_date", $"Pickup_DateTime").withColumn("locationID", $"PUlocationID").drop("Pickup_DateTime", "PUlocationID").sample(false, 0.001, 30)
  val fhv_all = unionPro(List(fhv2015, fhv2016, fhv2017_new), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("fhv")).select($"Pickup_date", $"locationID", $"City", $"taxi_type").filter($"locationID".isNotNull)


//************************fhvhv************************
  /**
   * fhvhv_all
   * root
   * |-- Pickup_date: string (nullable = true)
   * |-- locationID: string (nullable = true)
   * |-- City: string (nullable = false)
   * |-- taxi_type: string (nullable = false)
   *
   * fhvhv_all count = 2345645
   * fhvhv_all.filter($"locationID".isNotNull).count = 2345645
   */
  val fhvhv = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhvhv_tripdata_2019*").sample(false, 0.001, 30).withColumn("City", lit("New York")).withColumn("taxi_type", lit("fhvhv")).sample(false, 0.001, 30)
  val fhvhv_all = fhvhv.withColumn("Pickup_date", $"pickup_datetime").withColumn("locationID", $"PULocationID").select($"Pickup_date", $"locationID", $"City", $"taxi_type").filter($"locationID".isNotNull).filter($"Pickup_date".startsWith("2019"))

  val df = fhv_all.union(fhvhv_all).withColumn("Pickup_longitude", lit(null: String)).withColumn("Pickup_latitude", lit(null: String)).withColumn("Passenger_count", lit(null: String)).withColumn("Trip_distance", lit(null: String)).withColumn("Fare_amount", lit(null: String))
  import spark.implicits._

  val df_format = formatSchema(df)

//************************green************************
  /**
   *green_all.count = 794763
   *green_all
   *
  root
 |-- Pickup_date: string (nullable = true)
 |-- Pickup_longitude: string (nullable = true)
 |-- Pickup_latitude: string (nullable = true)
 |-- Passenger_count: string (nullable = true)
 |-- Trip_distance: string (nullable = true)
 |-- Fare_amount: string (nullable = true)
 |-- Trip_type: string (nullable = true)
 |-- PULocationID: string (nullable = true)  <----
 |-- DOLocationID: string (nullable = true)
 |-- City: string (nullable = false)
 |-- taxi_type: string (nullable = false)
   *
   */
  def greenSchema(df: DataFrame): DataFrame = {
    df.withColumn("Pickup_date", $"lpep_pickup_datetime").select($"Pickup_date", $"Pickup_longitude", $"Pickup_latitude", $"Passenger_count", $"Trip_distance", $"Fare_amount", $"Trip_type", $"PULocationID", $"DOLocationID", $"City", $"taxi_type")

  }
  val green2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2013*").filter($"lpep_pickup_datetime".contains("2013")).sample(false, 0.001, 30)
  val green2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2014*").filter($"lpep_pickup_datetime".contains("2014")).sample(false, 0.001, 30)
  val green2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2015*").filter($"lpep_pickup_datetime".contains("2015")).sample(false, 0.001, 30)
  val green2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2016*").filter($"lpep_pickup_datetime".contains("2016")).sample(false, 0.001, 30)
  val green2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2017*").filter($"lpep_pickup_datetime".contains("2017")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.001, 30)
  val green2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2018*").filter($"lpep_pickup_datetime".contains("2018")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.001, 30)
  val green2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2019*").filter($"lpep_pickup_datetime".contains("2019")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.001, 30)

  val green_all = greenSchema(unionPro(List(green2013, green2014, green2015, green2016, green2017, green2018, green2019), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("green"))).withColumn("locationID", $"PULocationID").drop("PULocationID").drop("Trip_type").drop("DOLocationID")


//************************yellow**************************
  /**
   * yellow_all
  root
 |-- Pickup_date: string (nullable = true)
 |-- Pickup_longitude: string (nullable = true)
 |-- Pickup_latitude: string (nullable = true)
 |-- Passenger_count: string (nullable = true)
 |-- Trip_distance: string (nullable = true)
 |-- Fare_amount: string (nullable = true)
 |-- PULocationID: string (nullable = true)  <----
 |-- DOLocationID: string (nullable = true)
 |-- City: string (nullable = false)
 |-- taxi_type: string (nullable = false)
   */

  def yellowSchema(df: DataFrame): DataFrame = {
    df.select($"Pickup_date", $"Pickup_longitude",$"Pickup_latitude", $"Passenger_count", $"Trip_distance", $"Fare_amount",  $"PULocationID", $"DOLocationID", $"City", $"taxi_type")
  }


  val yellow2009 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2009*").withColumn("Pickup_date", $"Trip_Pickup_DateTime").withColumn("Trip_distance", $"Trip_Distance").withColumn("Pickup_longitude", $"Start_Lon").withColumn("Pickup_latitude", $"Start_Lat").withColumn("Fare_amount", $"Fare_Amt").withColumn("Passenger_count", $"Passenger_Count").filter($"Trip_Pickup_DateTime".contains("2009")).sample(false, 0.001, 30)
  val yellow2010 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2010*").withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").filter($"pickup_datetime".contains("2010")).sample(false, 0.001, 30)
  val yellow2011 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2011*").filter($"pickup_datetime".contains("2011")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2012 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2012*").filter($"pickup_datetime".contains("2012")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2013*").filter($"pickup_datetime".contains("2013")).withColumn("Pickup_date", $"pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2014*").filter($" pickup_datetime".contains("2014")).withColumn("Pickup_date", $" pickup_datetime").withColumn("Passenger_count", $" passenger_count").withColumn("Trip_distance", $" trip_distance").withColumn("Pickup_longitude", $" pickup_longitude").withColumn("Pickup_latitude", $" pickup_latitude").withColumn("Fare_amount", $" fare_amount").sample(false, 0.001, 30)
  val yellow2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2015*").filter($"tpep_pickup_datetime".contains("2015")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2016*").filter($"tpep_pickup_datetime".contains("2016")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Pickup_longitude", $"pickup_longitude").withColumn("Pickup_latitude", $"pickup_latitude").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2017*").filter($"tpep_pickup_datetime".contains("2017")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2018*").filter($"tpep_pickup_datetime".contains("2018")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)
  val yellow2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2019*").filter($"tpep_pickup_datetime".contains("2019")).withColumn("Pickup_date", $"tpep_pickup_datetime").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").withColumn("Fare_amount", $"fare_amount").sample(false, 0.001, 30)

  val yellow_all = yellowSchema(unionPro(List(yellow2009, yellow2010, yellow2011, yellow2012, yellow2013, yellow2014, yellow2015, yellow2016, yellow2017, yellow2018, yellow2019), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("yellow"))).withColumn("locationID", $"PULocationID").drop("PULocationID").drop("DOLocationID")

  /**
   *
   * taxi_total_records

root
 |-- Pickup_date: string (nullable = true)
 |-- locationID: string (nullable = true)
 |-- City: string (nullable = false)
 |-- taxi_type: string (nullable = false)
 |-- Pickup_longitude: string (nullable = true)
 |-- Pickup_latitude: string (nullable = true)
 |-- Passenger_count: string (nullable = true)
 |-- Trip_distance: string (nullable = true)
 |-- Fare_amount: string (nullable = true)
 |-- Trip_type: string (nullable = true)
 |-- DOLocationID: string (nullable = true)
   */

  val df1 = green_all.union(yellow_all)
  val df1_format = formatSchema(df1)

  val taxi_total_records = df_format.union(filterUnreason(df1_format)) // <---

  val taxi_total_geo = taxi_total_records.join(taxi_gemo, taxi_total_records("locationID") === taxi_gemo("zone_id"), "left_outer").drop("zone_id")

  taxi_total_geo.repartition(1).write.mode(SaveMode.Overwrite).format("parquet").save("s3://newyorktaxitotal/taxiwithgeo/")





  taxi_total_records.repartition(1).write.mode(SaveMode.Overwrite).format("parquet").save("s3://newyorktaxitotal/sample_total")

  val taxi_total_gem =  taxi_total_records.join(taxi_gemo, taxi_total_records("locationID") === taxi_gemo("zone_id"), "left_outer")

  taxi_total_gem.write.mode(SaveMode.Overwrite).format("parquet").save("s3://newyorktaxitotal/totalsamplewithgemo")


}

/**
fhv_all.count = 138568
fhvhv_all.count = 250
 green_all.count = 79971
 yellow_all.count = 1611706

 fhv_all.show
 +-------------------+----------+--------+---------+
|        Pickup_date|locationID|    City|taxi_type|
+-------------------+----------+--------+---------+
|2015-01-05 23:24:00|       192|New York|      fhv|
|2015-01-06 11:45:00|       161|New York|      fhv|
|2015-01-16 11:50:00|       132|New York|      fhv|
|2015-01-01 05:18:00|       250|New York|      fhv|
|2015-01-03 17:03:00|       250|New York|      fhv|
|2015-01-04 05:06:00|       213|New York|      fhv|
|2015-01-04 06:14:00|       212|New York|      fhv|
|2015-01-04 16:47:00|        81|New York|      fhv|
|2015-01-04 17:15:00|       159|New York|      fhv|
|2015-01-05 08:43:00|       182|New York|      fhv|
|2015-01-05 11:04:00|       213|New York|      fhv|
|2015-01-06 08:01:00|       248|New York|      fhv|
|2015-01-06 15:07:00|       213|New York|      fhv|
|2015-01-06 22:12:00|       212|New York|      fhv|
|2015-01-08 14:32:00|       212|New York|      fhv|
|2015-01-09 14:04:00|       213|New York|      fhv|
|2015-01-09 23:46:00|       250|New York|      fhv|
|2015-01-11 16:28:00|       213|New York|      fhv|
|2015-01-12 22:38:00|       212|New York|      fhv|
|2015-01-13 22:40:00|       212|New York|      fhv|
+-------------------+----------+--------+---------+
only showing top 20 rows


fhvhv_all.show
 +-------------------+----------+--------+---------+
|        Pickup_date|locationID|    City|taxi_type|
+-------------------+----------+--------+---------+
|2019-02-01 20:45:28|       229|New York|    fhvhv|
|2019-02-01 21:31:21|        76|New York|    fhvhv|
|2019-02-04 00:45:30|        45|New York|    fhvhv|
|2019-02-05 19:21:56|       149|New York|    fhvhv|
|2019-02-06 14:39:49|        61|New York|    fhvhv|
|2019-02-07 14:18:38|       255|New York|    fhvhv|
|2019-02-08 13:13:42|       231|New York|    fhvhv|
|2019-02-08 18:35:37|       166|New York|    fhvhv|
|2019-02-09 10:31:12|        34|New York|    fhvhv|
|2019-02-09 20:06:46|       118|New York|    fhvhv|
|2019-02-09 23:43:54|       249|New York|    fhvhv|
|2019-02-10 14:39:24|       238|New York|    fhvhv|
|2019-02-15 16:40:21|       138|New York|    fhvhv|
|2019-02-17 12:25:13|       164|New York|    fhvhv|
|2019-02-18 19:02:48|       238|New York|    fhvhv|
|2019-02-19 20:12:29|        40|New York|    fhvhv|
|2019-02-21 21:05:34|       210|New York|    fhvhv|
|2019-02-22 08:06:25|        62|New York|    fhvhv|
|2019-02-22 18:55:20|        48|New York|    fhvhv|
|2019-02-22 22:02:10|       230|New York|    fhvhv|
+-------------------+----------+--------+---------+
only showing top 20 rows

green_all.show

 +-------------------+-------------------+------------------+---------------+-------------+-----------+---------+------------+--------+---------+----------+
|        Pickup_date|   Pickup_longitude|   Pickup_latitude|Passenger_count|Trip_distance|Fare_amount|Trip_type|DOLocationID|    City|taxi_type|locationID|
+-------------------+-------------------+------------------+---------------+-------------+-----------+---------+------------+--------+---------+----------+
|2013-09-01 21:09:31|-73.870384216308594|40.769935607910156|              1|         7.75|         23|     null|        null|New York|    green|      null|
|2013-09-01 22:17:42|   -73.903564453125|40.745220184326172|              2|         1.97|        8.5|     null|        null|New York|    green|      null|
|2013-09-04 16:14:48|-73.939430236816406|40.805149078369141|              5|         3.77|       15.5|     null|        null|New York|    green|      null|
|2013-09-07 00:10:25|-73.957916259765625|40.717658996582031|              1|         3.99|       19.5|     null|        null|New York|    green|      null|
|2013-09-07 06:15:37|-73.948760986328125|40.744548797607422|              1|          .00|        2.5|     null|        null|New York|    green|      null|
|2013-09-07 12:11:53|-73.963752746582031|40.808032989501953|              3|         2.00|          9|     null|        null|New York|    green|      null|
|2013-09-09 16:57:31|-73.844184875488281|40.721176147460938|              1|          .61|          5|     null|        null|New York|    green|      null|
|2013-09-09 20:34:05|-73.942955017089844|40.840137481689453|              1|         3.80|         20|     null|        null|New York|    green|      null|
|2013-09-10 12:11:54|-73.902091979980469|40.763645172119141|              1|          .00|        2.5|     null|        null|New York|    green|      null|
|2013-09-10 16:26:13|-73.844253540039062|40.720939636230469|              1|          .67|        5.5|     null|        null|New York|    green|      null|
|2013-09-12 20:05:24|-73.935600280761719|  40.8502197265625|              5|        14.80|         43|     null|        null|New York|    green|      null|
|2013-09-13 13:31:34|-73.959556579589844|40.702793121337891|              1|          .59|          4|     null|        null|New York|    green|      null|
|2013-09-14 16:09:17|   -73.920166015625| 40.86578369140625|              1|         1.65|          8|     null|        null|New York|    green|      null|
|2013-09-14 21:38:19|-73.948326110839844|40.786674499511719|              1|         2.12|        9.5|     null|        null|New York|    green|      null|
|2013-09-15 00:39:46|-73.925407409667969|40.762008666992188|              1|          .71|          5|     null|        null|New York|    green|      null|
|2013-09-15 02:07:19|-73.953010559082031|40.822723388671875|              2|          .50|          4|     null|        null|New York|    green|      null|
|2013-09-15 02:34:33|  -73.9512939453125|40.743724822998047|              1|         5.44|         17|     null|        null|New York|    green|      null|
|2013-09-15 13:25:20|-73.890998840332031|40.746974945068359|              1|         1.05|        6.5|     null|        null|New York|    green|      null|
|2013-09-15 18:38:50|-73.919052124023438|40.758861541748047|              1|         2.08|          9|     null|        null|New York|    green|      null|
|2013-09-15 19:08:16|-73.958869934082031|40.716739654541016|              1|         1.14|          6|     null|        null|New York|    green|      null|
+-------------------+-------------------+------------------+---------------+-------------+-----------+---------+------------+--------+---------+----------+
only showing top 20 rows

 yellow_all.show
 +-------------------+-------------------+------------------+---------------+-------------------+------------------+------------+--------+---------+----------+
|        Pickup_date|   Pickup_longitude|   Pickup_latitude|Passenger_count|      Trip_distance|       Fare_amount|DOLocationID|    City|taxi_type|locationID|
+-------------------+-------------------+------------------+---------------+-------------------+------------------+------------+--------+---------+----------+
|2009-01-08 21:12:00|-73.964716999999993|         40.764443|              1| 2.6000000000000001|9.3000000000000007|        null|New York|   yellow|      null|
|2009-01-09 23:42:00|                  0|                 0|              2|               1.99|              10.1|        null|New York|   yellow|      null|
|2009-01-23 23:58:00|-73.863365000000002|40.769843000000002|              1| 6.9100000000000001|16.899999999999999|        null|New York|   yellow|      null|
|2009-01-04 14:12:00|-73.965734999999995|40.753328000000003|              3| 2.3799999999999999|8.0999999999999996|        null|New York|   yellow|      null|
|2009-01-17 22:48:00|-74.000938000000005|40.729689999999998|              2|               1.97|11.300000000000001|        null|New York|   yellow|      null|
|2009-01-21 16:46:00|-73.959513000000001|40.766812999999999|              5|               2.48|9.3000000000000007|        null|New York|   yellow|      null|
|2009-01-29 13:23:00|-74.011683000000005|40.702939999999998|              1| 6.1299999999999999|15.699999999999999|        null|New York|   yellow|      null|
|2009-01-27 14:02:00|-73.957868000000005|         40.779398|              1|               1.96|8.9000000000000004|        null|New York|   yellow|      null|
|2009-01-10 02:03:00|-73.925141999999994|40.761963000000002|              1| 10.220000000000001|23.699999999999999|        null|New York|   yellow|      null|
|2009-01-09 16:28:00|-73.989104999999995|40.756860000000003|              1|               1.79|8.9000000000000004|        null|New York|   yellow|      null|
|2009-01-06 15:39:31|-73.137393000000003|41.366137999999999|              4|                1.5|8.0999999999999996|        null|New York|   yellow|      null|
|2009-01-09 22:26:29|         -73.996527|40.726854000000003|              1|0.90000000000000002|6.2000000000000002|        null|New York|   yellow|      null|
|2009-01-02 17:14:31|-73.992611999999994|40.749443999999997|              2|0.90000000000000002|7.0999999999999996|        null|New York|   yellow|      null|
|2009-01-23 13:56:31|-74.025523000000007|40.616354000000001|              1| 1.3999999999999999|               6.5|        null|New York|   yellow|      null|
|2009-01-12 17:18:15|         -73.977238|40.760102000000003|              1| 1.6000000000000001|8.3000000000000007|        null|New York|   yellow|      null|
|2009-01-13 11:04:12|-73.991675999999998|40.759574999999998|              1| 1.8999999999999999|              10.1|        null|New York|   yellow|      null|
|2009-01-12 09:30:49|-74.004205999999996|40.752555999999998|              1| 1.1000000000000001|6.0999999999999996|        null|New York|   yellow|      null|
|2009-01-26 09:02:59|-73.968692000000004|40.762447999999999|              1|                1.3|               6.5|        null|New York|   yellow|      null|
|2009-01-28 18:00:45|-73.988082000000006|40.766317000000001|              1|                1.7|8.6999999999999993|        null|New York|   yellow|      null|
|2009-01-26 14:10:16|-73.987105999999997|40.761375000000001|              1|0.80000000000000004|5.2999999999999998|        null|New York|   yellow|      null|
+-------------------+-------------------+------------------+---------------+-------------------+------------------+------------+--------+---------+----------+
only showing top 20 rows