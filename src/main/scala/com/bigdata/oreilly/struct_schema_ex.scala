package com.bigdata.oreilly

import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object struct_schema_ex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("struct_schema_ex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))



    val schema1 = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val data1=Seq(List((1,"Jules","Damji","https://tinyurl.1","1/4/2016",4535,Array("twitter","Linkedin")),
      (2,"Brooke","Wenig","https://tinyurl.2","5/5/2018",8908,Array("twitter","Linkedin")))
    )

    //val blogs_df1=spark.createDataFrame(data,schema1)

    val data=Seq((1,"Jules","Damji","https://tinyurl.1","1/4/2016",4535,Array("twitter","Linkedin")),
          (2,"Brooke","Wenig","https://tinyurl.2","5/5/2018",8908,Array("twitter","Linkedin"))
        )

    val blogs_df=spark.createDataFrame(data).toDF("id","First","Last","Url","Published","Hits","Campaigns")

    blogs_df.show()

   // val blogs_df1=spark.createDataFrame(data,schema1)


    spark.stop()
  }
}
