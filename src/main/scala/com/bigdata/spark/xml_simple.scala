package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


import org.apache.spark.sql.SparkSession
object xml_simple {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("xml_simple").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

        val data = "file:///c:\\bigdata\\datasets\\xmldata\\books.xml"

        val df=spark.read.format("com.databricks.spark.xml").option("rowTag","book").load(data).withColumnRenamed("_id","id")

        df.createOrReplaceTempView("tab")
        df.show()
        df.printSchema()
        val res = spark.sql("select genre, count(*) cnt from tab group by genre ")
        res.show()

    //anywhere if u get this error .. it means dependency not avail go to spark packages and download that dependencies
    //Failed to find data source: xml. Please find packages at http://spark.apache.org/third-party-projects.html


    spark.stop()
  }
}
