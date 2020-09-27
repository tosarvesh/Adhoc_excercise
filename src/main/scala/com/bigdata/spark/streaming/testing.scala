package com.bigdata.spark.streaming
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.log4j._
object testing {
  case class aslcc(name:String, age:Int, city:String)
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //  val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql
    // Create a DStream that will connect to hostname:port, like localhost:9999
    //get data from terminal from 1234 port
    //val lines = ssc.socketTextStream("ec2-3-22-250-115.us-east-2.compute.amazonaws.com", 1235)
    //nc -lk 1235 run on vm machine or amazon ec2 sarvesh,19,del sample data for streaming
    val lines = ssc.socketTextStream("192.168.46.134", 1234)

    //lines.print()
    lines.foreachRDD{x=>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._ //implicits ... convert rdd to dataframe or dataset or anyother use this

      // Convert RDD[String] to DataFrame
      val df = x.map(x=>x.split(",")).map(x=>aslcc(x(0),x(1).toInt,x(2))).toDF()

      // Create a temporary view
      df.createOrReplaceTempView("tab")
      val hyd = spark.sql("select * from tab where city='hyd'")
      val mas = spark.sql("select * from tab where city='mas'")
      val del = spark.sql("select * from tab where city='del'")
      val other = spark.sql("select * from tab where city not in ('mas','hyd','del')")
      hyd.show()
      other.show()
     /* val oprop = new java.util.Properties()
      oprop.setProperty("user","ousername")
      oprop.setProperty("password","opassword")
      oprop.setProperty("driver","oracle.jdbc.OracleDriver")
      val host = "jdbc:oracle:thin:@//mydatabase.cmw3falnhysb.ap-south-1.rds.amazonaws.com:1521/ORCL"
      hyd.write.mode(SaveMode.Append).jdbc(host,"hydinfo",oprop)
      mas.write.mode(SaveMode.Append).jdbc(host,"masinfo",oprop)
      del.write.mode(SaveMode.Append).jdbc(host,"delhiinfo",oprop)
      other.write.mode(SaveMode.Append).jdbc(host,"otherstate",oprop)
*/
      df.show()
    }

    //spark.stop()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}