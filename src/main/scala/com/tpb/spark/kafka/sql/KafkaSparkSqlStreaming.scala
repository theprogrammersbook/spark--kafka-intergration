package com.tpb.spark.kafka.sql

import com.tpb.spark.kafka.common.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, current_timestamp, from_json, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KafkaSparkSqlStreaming {

  def main(args: Array[String]): Unit = {
    @transient val spark: SparkSession = SparkSession.builder().config(ConfigLoader.sparkConf).getOrCreate()

    import spark.implicits._

    val records = spark.
      readStream.
      format("kafka").
      option("subscribepattern", "persons").
    //.option("subscribePattern", "topic.*")
    //subscribe	A comma-separated list of topics	The topic list to subscribe. Only one of "assign", "subscribe" or
      // "subscribePattern" options can be specified for Kafka source.
      option("kafka.bootstrap.servers", "localhost:9092").
    /*
    "latest" for streaming, "earliest" for batch
     */
      option("startingoffsets", "latest").
    //Use maxOffsetsPerTrigger option to limit the number of records to fetch per trigger.
      option("maxOffsetsPerTrigger", 10).
      load

    val schema = StructType(Seq(StructField("id", IntegerType),
      StructField("person", StructType(Seq(StructField("name", StringType))))))

    val result = records.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")


    val ints = result
      .withColumn("t", current_timestamp())
      .withWatermark("t", "1 minutes")
      .groupBy(window($"t", "1 minutes") as "window")
      .agg(count("*") as "total")

    ints.printSchema()
    val see = ints.writeStream.
      format("console").
      option("truncate", value = false).
      trigger(Trigger.ProcessingTime(10)).
      outputMode(OutputMode.Complete()).
      queryName("from-kafka-to-console").
      start
    see.awaitTermination()
  }
}
