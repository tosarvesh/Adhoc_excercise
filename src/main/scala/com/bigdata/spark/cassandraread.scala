package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/*
1-go to anaconda prompt

Cassandra is compatible with 2.7 so need to create virtual environment

2- conda create --name py27 python=2.7

actiavet the environment

3-conda activate py27


load cassandra
4-(py27) C:\Users\satis>cassandra -f


5-open another anaconda command promp
conda activate py27
cqlsh


cassandra -f

cqlsh

CREATE KEYSPACE January
    WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

	DESCRIBE KEYSPACES

	USE January;

create table asl (
id INT PRIMARY KEY,
name varchar,
city varchar
);

insert into asl (id, name, city) values(1,'venu','hyderabad');
insert into asl (id, name, city) values(2,'sita','hyderabad');
insert into asl (id, name, city) values(3,'subbu','hyderabad');
insert into asl (id, name, city) values(4,'brahma','hyderabad');
insert into asl (id, name, city) values(5,'nirmal','bangalore');
insert into asl (id, name, city) values(6,'nischal','bangalore');
insert into asl (id, name, city) values(7,'pramod','bangalore');
insert into asl (id, name, city) values(8,'anu','hyderabad');
insert into asl (id, name, city) values(9,'ali','chennai');
insert into asl (id, name, city) values(10,'koti','chennai');
insert into asl (id, name, city) values(11,'venkat','chennai');

	select * from asl where city='hyderabad';
 select * from asl where id=2;





 reate table nep (
 phone INT,
 name varchar,
 email varchar,
 primary key (phone,name,email)
 );

 insert into nep (phone, name, email) values(123456363,'venu','venu@gmail.com');
 insert into nep (phone, name, email) values(2243563,'sita','sita@yahoo.com');
 insert into nep (phone, name, email) values(3563737,'subbu','subbu@gmail.com');
insert into nep (phone, name, email) values(45536373,'brahma','vijay@gmail.com');


create table nep (
phone INT,
name varchar,
email varchar,
primary key (phone,name,email)
);

insert into nep (phone, name, email) values(123456363,'venu','venu@gmail.com');
insert into nep (phone, name, email) values(2243563,'sita','sita@yahoo.com');
insert into nep (phone, name, email) values(3563737,'subbu','subbu@gmail.com');
insert into nep (phone, name, email) values(45536373,'brahma','vijay@gmail.com');





create table asljoinnep(
  id INT PRIMARY KEY,
  city varchar,
  name varchar,
  phone INT,
  email varchar,
 );




*/
object cassandraread {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("cassandraread").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   // val df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///home/hadoop/Desktop/inc.csv")
    //df.write.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","asl").mode("append").save()
    //test
    //read from cassandra
    val asldf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","asl").load()
    asldf.show()
    val nepdf = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","nep").load()
    nepdf.show()
    asldf.createOrReplaceTempView("asl")
    nepdf.createOrReplaceTempView("nep")
    val res=spark.sql("select a.*,n.phone,n.email from asl a join nep n on a.name=n.name")
     res.show()
//need to create table in advance
    res.write.format("org.apache.spark.sql.cassandra").option("keyspace","january").option("table","asljoinnep").mode("append").save()

    spark.stop()
  }
}
