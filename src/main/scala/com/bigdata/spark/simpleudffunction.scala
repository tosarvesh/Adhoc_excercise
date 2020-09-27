package com.bigdata.spark
import org.apache.log4j._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object simpleudffunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("simpleudffunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  //  val sc = spark.sparkContext

    import spark.sql
    import org.apache.log4j._
    import org.apache.log4j.Logger
    import spark.implicits._

  //  def inc_by_two(x: Int):Int={x+2}

//    val plusthree = (x: Int) => x + 3 or using below methos it's same thing

    val plusthree = udf((x: Int) => x + 3)
    spark.udf.register("plusthree", plusthree)
    spark.sql("SELECT plusthree(5)").show()


    val inc_by_tw = udf(inc_by_two _)

    spark.udf.register("plustwo",inc_by_tw)
    spark.sql("SELECT plustwo(5)").show()




    def inc_by_one=(x: Int)=>{x+1}

    spark.udf.register("plusone",inc_by_one)
    spark.sql("SELECT plusone(5)").show()


    // UDF in a WHERE clause
    spark.udf.register("oneArgFilter", (n: Int) => { n > 5 })
    spark.range(1, 10).createOrReplaceTempView("test")
    spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show()






    spark.stop()
  }


  def inc_by_two(x: Int):Int={x+2}
}
