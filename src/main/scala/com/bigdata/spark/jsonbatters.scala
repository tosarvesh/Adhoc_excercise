package com.bigdata.spark
/*
Task -2
Read the data and validate in google thru valid or not.
Read the thru DF ( Dataframe)
show ()
f.printSchema()
assign withcolumn and flatten the data.
bring the data set format.
bring the count data
*/

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jsonbatters {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsonbatters").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql


    val data = "file:///c:\\bigdata\\datasets\\jsondata\\batters.json"

    val df=spark.read.format("json").option("multiline","true").load(data)
    df.createOrReplaceTempView("tab")
    // df.show()
    df.printSchema()
    val res=  df.withColumn("results",explode($"results")).withColumn("topping",explode($"results.topping")).withColumn("batter",explode($"results.batters.batter"))
      .select($"results.id",$"results.type",$"results.name",$"results.ppu",$"topping.id",$"topping.type",$"batter.id",$"batter.type")

res.show()
    /*
        df.select($"id",$"type",$"name",$"image.height".as("image height"),$"image.url".as("Image url"),$"image.width".as("Image width"),$"thumbnail.height".as("Thumbnail height"),$"thumbnail.url".as("Thumbnail url"),$"thumbnail.width".as("Thumbnail width")).show()

        val res=  df.withColumn("Image_height",$"image.height").select($"id",$"type",$"name",$"image_height",col("image.width").as("image_width"))
        res.show()


        val cnt1=df.select("id").groupBy("Id").count()
        cnt1.show()

        val cnt2=df.select("id").groupBy("Id").agg(count("*"))
        cnt2.show()


        df.agg(count("*").as("Count")).show()

        val col_cnt=df.columns.size

        println("Total number of Columns: "+col_cnt)

    */


    spark.stop()
  }
}
