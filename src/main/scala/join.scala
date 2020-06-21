import org.apache.spark.sql.SparkSession

object join {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "/home/niranda/software/hadoop-2.10.0")

    val spark = SparkSession
      .builder()
      .appName("Example")
      //      .config("spark.master", "local[4]")
      .config("spark.master", args(0))
      .getOrCreate()

    val parallelism = args(1).toInt

    val leftDf = spark.read.format("csv").option("header", value = true)
      .load("hdfs://localhost:9001/test/csv1_*")
      .repartition(parallelism).cache()
    println("#### left_df " + leftDf.count())

    val rightDf = spark.read.format("csv").option("header", value = true)
      .load("hdfs://localhost:9001/test/csv2_*")
      .repartition(parallelism).cache()
    println("#### right_df " + leftDf.count())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println(spark.conf.get("spark.sql.join.preferSortMergeJoin"))
    //    csvDf.printSchema()

    val t0 = System.nanoTime()
    val q = leftDf.join(rightDf, Seq("0"))
    val lines = q.count()
    val t1 = System.nanoTime()

    println("#### time sm " + (t1 - t0) / 1e6 + " lines " + lines)
    //    Thread.sleep(1000000000) // wait for 1000 millisecond
  }

}
