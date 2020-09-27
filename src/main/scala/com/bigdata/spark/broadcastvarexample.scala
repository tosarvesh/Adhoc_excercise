package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//accumulator
object broadcastvarexample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("broadcastvarexample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

val blankLines = sc.accumulator(0,"Blank Lines")
    sc.textFile("file:///c:\\bigdata\\datasets\\broadcastvarex.csv").foreach { line =>
      if (line.length() == 0) blankLines += 1
    }



println(s"\tBlank Lines=${blankLines.value}")

    spark.stop()
  }
}
