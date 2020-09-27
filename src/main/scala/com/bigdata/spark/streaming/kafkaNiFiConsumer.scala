package com.bigdata.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._

//https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10_2.11
object kafkaNiFiConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("sparkKafkaIntegration").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    // val sc = spark.sparkContext
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("indpak")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record =>  record.value)  // dstream
    lines.print()
    lines.foreachRDD{x=>
      val tab = x.filter(x=>x.nonEmpty)
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._ //implicits ... convert rdd to dataframe or dataset or anyother use this

      // Convert RDD[String] to DataFrame
      //val df = x.map(x=>x.split(",")).map(x=>aslcc(x(0),x(1).toInt,x(2))).toDF()
      val df = spark.read.json(tab)

      // Create a temporary view
      /* df.createOrReplaceTempView("tab")
       val hyd = spark.sql("select * from tab where city='hyd'")
       val mas = spark.sql("select * from tab where city='mas'")
       val del = spark.sql("select * from tab where city='del'")
       val other = spark.sql("select * from tab where city not in ('mas','hyd','del')")
       hyd.show()
       other.show()
       val oprop = new java.util.Properties()
       oprop.setProperty("user","ousername")
       oprop.setProperty("password","opassword")
       oprop.setProperty("driver","oracle.jdbc.OracleDriver")
       val host = "jdbc:oracle:thin:@//mydatabase.cmw3falnhysb.ap-south-1.rds.amazonaws.com:1521/ORCL"
       hyd.write.mode(SaveMode.Append).jdbc(host,"hydinfo_devi",oprop)
       mas.write.mode(SaveMode.Append).jdbc(host,"masinfo_devi",oprop)
       del.write.mode(SaveMode.Append).jdbc(host,"delhiinfo_devi",oprop)
       other.write.mode(SaveMode.Append).jdbc(host,"otherstate_devi",oprop)*/

      df.show()
    }


    ssc.start()
    ssc.awaitTermination()
  }
  case class aslcc(name:String, age:Int, city:String)
}
