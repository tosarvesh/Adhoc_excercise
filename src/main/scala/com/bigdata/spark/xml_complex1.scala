package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object xml_complex1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("xml_complex1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///c:\\bigdata\\datasets\\xmldata\\xmldata.xml"
    val df=spark.read.format("xml").option("rowTag","course").load(data).withColumnRenamed("_id","id")
    df.show()
    df.createOrReplaceTempView("tab")
    /*
    +----+-----+-----------+--------------------+-------+----+----+----------------+--------------------+-----+
   |crse| days| instructor|               place|reg_num|sect|subj|            time|               title|units|
    +----+-----+-----------+--------------------+-------+----+----+----------------+--------------------+-----+
   | 211|  M-W|  Brightman|        [ELIOT, 414]|  10577| F01|ANTH|[04:30, 03:10PM]|Introduction to A...|  1.0|
   */


    val df1 = df.select("instructor","place.*","time.*").drop("place","time")
    //.withColumn("end_time", to_date($"end_time"))
    //.withColumn("start_date",($"start_time"))
    df1.show()
    df1.printSchema()
    //    df.show()
    df.printSchema()
    spark.stop()
  }
}