name := "Adhoc_excercise"

version := "0.3"

scalaVersion := "2.11.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "com.databricks" % "spark-xml_2.11" % "0.10.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4"
