package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object shuffling_example {
  def main(args: Array[String]) {
 try {
   val spark = SparkSession.builder.master("local[*]").appName("shuffling_example").getOrCreate()
   //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
   val sc = spark.sparkContext

   import spark.implicits._
   import spark.sql
   val df = spark.read.schema(
     """
        order_id INT,
        order_date STRING,
        order_customer_id INT,
        order_status STRING
        """).csv("file:///C:\\bigdata\\retail_db\\orders")
   println("partitions " + df.rdd.partitions.length)
   df.printSchema()

   println(df.groupBy("order_id").count().rdd.partitions.length)
   //200 partitions because the parameter spark.sql.shuffle.partitions which controls number of shuffle partitions is set to 200 by default
   spark.conf.set("spark.sql.shuffle.partitions", 100)
   println(df.groupBy("order_id").count().rdd.partitions.length)

   //The exact logic for coming up with number of shuffle partitions depends on actual analysis. You can typically set it to be 1.5 or 2 times of the initial partitions.
   spark.stop()
 }catch {
   case _: Exception => {println("Exception") }
   case e: Exception => e.printStackTrace()}

 finally {println("End")}


  }
}
