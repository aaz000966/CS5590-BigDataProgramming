
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.array
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes._


object object2{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("GraphCode")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = {
      SparkSession.builder().appName("GraphCode").config("spark.master", "local").getOrCreate()
    }

    val StationRDD = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv("Datasets/201508_station_data.csv")

    val TripRDD = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv("Datasets/201508_trip_data.csv")

    val ConsumerRDD = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv("Datasets/ConsumerComplaints.csv")


    val StationRDD2 = StationRDD.withColumn("geo_data", array("lat", "long"))


    val StationRDDND = StationRDD.dropDuplicates()
    val TripRDDND = TripRDD.dropDuplicates()


    val station_data = StationRDD.withColumnRenamed("station_id", "id")
    val trip_data = TripRDD.withColumnRenamed("Start Terminal", "src").withColumnRenamed("End Terminal", "dst").withColumnRenamed("Trip ID", "trip")

    println("Stations data frame with new column name:")
    station_data.show(5)

    println("Trips data frame with new columns names:")
    trip_data.show(5)


    station_data.createOrReplaceTempView("stations")
    trip_data.createOrReplaceTempView("trips")

    //to create vertices data frame
    val Rows = spark.sql("SELECT id, name FROM stations")
    val vertices = Rows.rdd
    vertices.saveAsTextFile("output/vertices.txt")
    //to create edges data frame
    val ERows = spark.sql("SELECT src, dst, trip FROM trips")
    val edges = ERows.rdd
    edges.saveAsTextFile("output/edges.txt")


    val myGraph = GraphFrame(Rows , ERows)


    val in_Degree = myGraph.inDegrees
    in_Degree.show(5)


    val out_Degree = myGraph.outDegrees
    out_Degree.show()

    val motifs: DataFrame = myGraph.find("(a)-[ERows]->(b); (b)-[e2]->(a)")
    motifs.show()

    println("Here!")

    val v2 = myGraph.vertices.filter("id > 20")
    val e2 = myGraph.edges.filter("dst > 50")
    val myGraph2 = GraphFrame(v2,e2)

    println(myGraph2)

  }
}

