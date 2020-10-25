package com.tpb.spark.kafka.sql

import org.apache.spark.sql.SparkSession

object ReadJSON extends App {
   val spark= SparkSession.builder()
    .appName("ReadJSON")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.read.json("src/main/scala/com/tpb/spark/kafka/sql/records.json").show()
}
