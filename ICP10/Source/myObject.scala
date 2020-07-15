import org.apache.spark._
import org.apache.spark.sql.SparkSession

object myObject {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Survey")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = {
      SparkSession.builder().appName("Survey").config("spark.master", "local").getOrCreate()
    }

    val myDF = spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv("input/survey.csv")


    myDF.write.format("com.databricks.spark.csv").save("output/myDF/myDF.csv")


    val distinctDF = myDF.distinct()
    println("before duplicates: ")
    println(myDF.count())
    println("After:")
    println(distinctDF.count())


    val myDFSplit = myDF.randomSplit(Array(1, 1))
    val Split1 = myDFSplit(0)
    val Split2 = myDFSplit(1)

    val myDFUnion = Split1.union(Split2)


    val orderbycountry = myDFUnion.orderBy("Country")
    println("Head of the DF order by country:")
    println(orderbycountry.show(10))


    myDF.createOrReplaceTempView("survey")
    val Q1 = spark.sql("SELECT treatment, COUNT(treatment) AS responses from survey GROUP BY treatment")
    println("query 1  result")
    Q1.show(10)


    val Q2 = spark.sql("SELECT Country, MAX(Age) AS maxAge FROM survey GROUP BY Country ORDER BY maxAge DESC")
    println("query 2  result")
    Q2.show()

    val Q3 = spark.sql("SELECT state, Age, COUNT(Age) as numberOfCasesPerAge FROM survey WHERE " +
      "(Gender=='Female' OR Gender=='F' OR Gender=='female' OR Gender=='f') AND (seek_help=='Yes') " +
      "GROUP BY state, Age ORDER BY numberOfCasesPerAge DESC")

    println("query 3  result")
    Q3.show()



    val res = myDF.rdd.take(13).last
    println("13th row")
    print(res.toString())



  }
}