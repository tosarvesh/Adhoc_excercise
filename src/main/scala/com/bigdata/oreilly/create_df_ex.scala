package com.bigdata.oreilly

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object create_df_ex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("create_df_ex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

val dataDF=spark.createDataFrame(Seq(("Brooke",20),("Brooke",25),("Denny",31))).toDF("name","age")
dataDF.select("name","age").show()
val avgDF=dataDF.groupBy("name").agg(avg("age"))
avgDF.show()



    spark.stop()
  }
}
