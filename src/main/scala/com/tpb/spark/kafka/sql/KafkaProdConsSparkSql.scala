package com.tpb.spark.kafka.sql

import com.tpb.spark.kafka.common.ConfigLoader.kafkaServer
import SparkWithKafka._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by nagaraju on 6/7/17.
  */
object KafkaProdConsSparkSql {

  val spark: SparkSession = SparkSession.builder().master("local[2]").appName("test").getOrCreate()

  def main(args: Array[String]): Unit = {
     //creating kafka topic, which needs to be created already.
    val kafkaTopic = "persons"

    val nestedColumn = Array(col("id"), struct(col("name")).alias("person"))
    val sampleDF = spark.createDataFrame(Seq((1001, "Akash"), (1002, "Rishabh"), (1003, "Kunal")))
      .toDF("id", "name").select(nestedColumn: _*)
    sampleDF.writeToKafka(kafkaServer, kafkaTopic)
    spark.read.readFromKafka(kafkaServer, kafkaTopic).show
  }
}
