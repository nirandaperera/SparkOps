import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Example")
      //.config("spark.master","local")
      .config("spark.master", args(0))
      .getOrCreate()

    val t0 = System.nanoTime()
    val csvDf = spark.read.format("csv").option("header",true).load("hdfs://localhost:9000/test/mycsv1.csv")

    csvDf.createOrReplaceTempView("csvTable")
    //spark.sqlContext.sql("select * from csvTable").show()
    val csvDf1 = csvDf.cache()
    val t1 = System.nanoTime()

    //println("Lines: " + csvDf.count())
    println("Elapsed time to read: " + (t1 - t0)/10e8 + "s")

    val t3 = System.nanoTime()
    csvDf.write
      .option("header", true)
      .csv("/home/hasara/python code/csv/test1")
    val t4 = System.nanoTime()
    println("Elapsed time to write: " + (t4 - t3)/10e8 + "s")
  }


}
