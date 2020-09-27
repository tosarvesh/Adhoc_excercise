package com.bigdata.spark
import org.apache.log4j._
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object udffunctions {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local[*]").appName("udffunctions").getOrCreate()
Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("org").setLevel(Level.ERROR)

    Logger.getLogger("org").info("INside main class ")

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.sql
    import org.apache.log4j._
    import org.apache.log4j.Logger
    import spark.implicits._

        val data = "C:\\bigdata\\Datasets\\bank-full.csv"
        val df = spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load(data)
        df.show()
        df.createOrReplaceTempView("tab") // to run sql queries on top of dataframe use createOrReplaceTempView
        //val res  = spark.sql("select *, REGEXP_REPLACE(job,'-','') newjob from tab where balance>50000 and age>50 and   marital!='married'")
        // .drop("job").withColumnRenamed("newjob","job")
        //val res  = spark.sql("select age, REGEXP_REPLACE(job,'-','') job, marital,education,default,balance,housing,loan,contact,day,month,duration,campaign,pdays,previous,poutcome,y from tab where balance>50000 and age>50 and   marital!='married'")
      println("with column")
     //    val res1 = df.withColumn("job", regexp_replace(upper($"job"),"[^a-zA-Z]",""))
         val res1 = df.withColumn("job", regexp_replace(upper($"job"),"[^a-zA-Z]","")).withColumn("comm",round($"balance"*0.2)).withColumn("seq",monotonically_increasing_id()+1).drop("housing")

        // withColumn ... if already column exists its update column values. if column not exists add new column.
         res1.show()
        // val res = df.orderBy($"balance".desc).withColumn("tmp",monotonically_increasing_id()+1).where($"tmp"===10).drop("tmp")
        // mobotonically_increate_id add unique auto increase manner add 1,2,3,4 like this numbes add
    println("with udf")
        val uoff = udf(ageoffer _) // scala method/function convert to udf this is not required if in the function you are using spark action notation => as in udffunctions2

        //    val res=df.withColumn("todayoffer",uoff($"age"))
        spark.udf.register("ageoffer", uoff)

    val res = spark.sql("select *, ageoffer(age) weekendoffer from tab")
        res.show()
        spark.stop()
      }

      def ageoffer(age:Int) = age match {
        case age if(age>18 && age<30) => "30% off"
        case age if(age>=30 && age<=50) => "40% off"
        case age if(age>50 && age<70) => "70% off"
        case _ => "no offer"
      }
    }




