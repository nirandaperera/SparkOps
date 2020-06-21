import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
object union {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Example")
      //.config("spark.master","local")
      .config("spark.master", args(0))
      .getOrCreate()
    var csvDf = spark.read.format("csv").option("header", true).load("hdfs://localhost:9000/test/mycsv1.csv")
    var csvDf1 = csvDf.cache()

    var csvDf2 = spark.read.format("csv").option("header",true).load("hdfs://localhost:9000/test/mycsv2.csv")
    val csvDf3 = csvDf2.cache()

    val t0 = System.nanoTime()
    //csvDf1 = csvDf1.union(csvDf3)
    csvDf1 = csvDf1.intersect(csvDf3)
    csvDf1 = csvDf1.cache()
    val t1 = System.nanoTime()
    println(csvDf.count())
    println("Elapsed time: " + (t1 - t0)/10e8 + "s")
  }

}
