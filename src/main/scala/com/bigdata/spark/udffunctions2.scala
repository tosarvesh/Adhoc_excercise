package com.bigdata.spark

import org.apache.log4j.{Logger, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession


object udffunctions2 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local[*]").appName("udffunctions").getOrCreate()
Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("org").setLevel(Level.ERROR)

    Logger.getLogger("org").info("INside main class ")

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._

        val data = "C:\\bigdata\\Datasets\\bank-full.csv"
        val df = spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load(data)
        df.show()
        df.createOrReplaceTempView("tab") // to run sql queries on top of dataframe use createOrReplaceTempView
      println("with column")

      println("with udf")




    def ageoffer=(age:Int) => {
      age match {
      case age if (age > 18 && age < 30) => "30% off"
      case age if (age >= 30 && age <= 50) => "40% off"
      case age if (age > 50 && age < 70) => "70% off"
      case _ => "no offer"
      }
    }
       // val uoff = udf(ageoffer _) // scala method/function convert to udf this is not required if in the function you are using spark action notation => like above

        //    val res=df.withColumn("todayoffer",uoff($"age"))
        spark.udf.register("ageoffer", ageoffer)

    val res = spark.sql("select *, ageoffer(age) weekendoffer from tab")
        res.show()

    val squared = (s: Int) => {
      s * s
    }
    spark.udf.register("square", squared)
    spark.range(1, 20).createOrReplaceTempView("test")
    val res2=spark.sql("select id, square(id) as id_squared from test")
    res2.show()

    def cube=(x: Int)=>{x*x*x}


    spark.udf.register("cube", cube)
    spark.range(1, 20).createOrReplaceTempView("test2")
    val res3=spark.sql("select id, cube(id) as id_squared from test2")
    res3.show()

  //  val dff=spark.range(1, 20)
//dff.select(squared(col("id"))).as("squaredid").show()

    val dataset = Seq((0, "hello"), (1, "world")).toDF("id", "text")
    val upperUDF = udf(upper)
    dataset.withColumn("upper", upperUDF('text)).show


    spark.stop()
      }

  val upper: String => String = _.toUpperCase

}




