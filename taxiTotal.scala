import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
   * filter unreasonable data
   */

  def filterUnreason(df: DataFrame): DataFrame = {
    import spark.implicits._
    df.filter($"Passenger_count" === "0").filter($"Fare_amount".startsWith("-"))
  }

  /**
   * add the taxi_zone_geom information
   */

  val taxi_zone_gemo = spark.read.format("com.databricks.spark.csv").option("header", "true").load("./taxi_zone_geom.csv")

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

  val fhv2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2015*").filter($"Pickup_date".startsWith("2015")).sample(false, 0.01, 30)
  val fhv2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2016*").filter($"Pickup_date".startsWith("2016")).sample(false, 0.01, 30)
  val fhv2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhv_tripdata_2017*").filter($"Pickup_date".startsWith("2017")).sample(false, 0.01, 30)
  val fhv2017_new = fhv2017.withColumn("Pickup_date", $"Pickup_DateTime").withColumn("locationID", $"PUlocationID").drop("Pickup_DateTime", "PUlocationID")
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
  val fhvhv = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/fhvhv_tripdata_2019*").sample(false, 0.01, 30).withColumn("City", lit("New York")).withColumn("taxi_type", lit("fhvhv"))
  val fhvhv_all = fhvhv.withColumn("Pickup_date", $"pickup_datetime").withColumn("locationID", $"PULocationID").select($"Pickup_date", $"locationID", $"City", $"taxi_type").filter($"locationID".isNotNull).filter($"Pickup_date".startsWith("2019"))

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
 |-- PULocationID: string (nullable = true)
 |-- DOLocationID: string (nullable = true)
 |-- City: string (nullable = false)
 |-- taxi_type: string (nullable = false)
   *
   */
  def greenSchema(df: DataFrame): DataFrame = {
    df.withColumn("Pickup_date", $"lpep_pickup_datetime").select($"Pickup_date", $"Pickup_longitude", $"Pickup_latitude", $"Passenger_count", $"Trip_distance", $"Fare_amount", $"Trip_type", $"PULocationID", $"DOLocationID", $"City", $"taxi_type")

  }
  val green2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2013*").filter($"lpep_pickup_datetime".startsWith("2013")).sample(false, 0.01, 30)
  val green2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2014*").filter($"lpep_pickup_datetime".startsWith("2014")).sample(false, 0.01, 30)
  val green2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2015*").filter($"lpep_pickup_datetime".startsWith("2015")).sample(false, 0.01, 30)
  val green2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2016*").filter($"lpep_pickup_datetime".startsWith("2016")).sample(false, 0.01, 30)
  val green2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2017*").filter($"lpep_pickup_datetime".startsWith("2017")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.01, 30)
  val green2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2018*").filter($"lpep_pickup_datetime".startsWith("2018")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.01, 30)
  val green2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/green_tripdata_2019*").filter($"lpep_pickup_datetime".startsWith("2019")).withColumn("Fare_amount", $"fare_amount").withColumn("Trip_type", $"trip_type").withColumn("Passenger_count", $"passenger_count").withColumn("Trip_distance", $"trip_distance").sample(false, 0.01, 30)

  val green_all = greenSchema(unionPro(List(green2013, green2014, green2015, green2016, green2017, green2018, green2019), spark).withColumn("City", lit("New York")).withColumn("taxi_type", lit("green")))


//************************yellow**************************
val yellow2009 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2009*").sample(false, 0.01, 30)
val yellow2010 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2010*").sample(false, 0.01, 30)
val yellow2011 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2011*").sample(false, 0.01, 30)
val yellow2012 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2012*").sample(false, 0.01, 30)
val yellow2013 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2013*").sample(false, 0.01, 30)
val yellow2014 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2014*").sample(false, 0.01, 30)
val yellow2015 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2015*").sample(false, 0.01, 30)
val yellow2016 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2016*").sample(false, 0.01, 30)
val yellow2017 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2017*").sample(false, 0.01, 30)
val yellow2018 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2018*").sample(false, 0.01, 30)
val yellow2019 = spark.read.format("com.databricks.spark.csv").option("header", "true").load("s3://nyc-tlc/trip data/yellow_tripdata_2019*").sample(false, 0.01, 30)




}
