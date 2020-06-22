import org.apache.spark.sql.SparkSession

object table_join_dist_test {

  // args: 4 hdfs://v-login1:9001/twx spark://localhost:7077
  def main(args: Array[String]): Unit = {

    val parallelism = args(0).toInt
    val inputDir = args(1) // "hdfs://localhost:9001/test"

    println("#### spark dist_join workers: " + parallelism)

    val spark = SparkSession
      .builder()
      .appName("Spark join " + parallelism)
      .config("spark.master", args(2))
      .getOrCreate()

    val leftDf = spark.read.format("csv").option("header", value = true)
      .load(inputDir + "/csv1_*")
      .repartition(parallelism).cache()
    println("#### spark left_df " + leftDf.count())

    val rightDf = spark.read.format("csv").option("header", value = true)
      .load(inputDir + "/csv2_*")
      .repartition(parallelism).cache()
    println("#### spark right_df " + leftDf.count())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.get("spark.sql.join.preferSortMergeJoin")
    //    csvDf.printSchema()

    val t0 = System.nanoTime()
    val q = leftDf.join(rightDf, Seq("0"))
    val lines = q.count()
    val t1 = System.nanoTime()

    println("#### spark join time ms " + ((t1 - t0) / 1e6).toInt + " lines " + lines + " para " +
      parallelism)
    //    Thread.sleep(1000000000) // wait for 1000 millisecond
  }

}
