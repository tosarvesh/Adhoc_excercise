package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object udfexample1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("udfexample1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql


    val userData = spark.createDataFrame(Seq(
      (1, "Chandler", "Pasadena", "US"),
      (2, "Monica", "New york", "USa"),
      (3, "Phoebe", "Suny", "USA"),
      (4, "Rachael", "St louis", "United states of America"),
      (5, "Joey", "LA", "Ussaa"),
      (6, "Ross", "Detroit", "United states")
    )).toDF("id", "name", "city", "country")


    val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")

    def cleanCountry = (country: String) => {
      val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")
      if (allUSA.contains(country)) {
        "USA"
      }
      else {
        "unknown"
      }
    }


    val normaliseCountry = spark.udf.register("normalisedCountry",cleanCountry)
    //spark is the SparkSession here.Available by default in spark-shell


    userData.withColumn("normalisedCountry",normaliseCountry(col("country"))).show





    spark.stop()
  }
}
