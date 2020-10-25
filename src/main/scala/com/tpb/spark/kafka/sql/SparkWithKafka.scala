package com.tpb.spark.kafka.sql

import com.tpb.spark.kafka.common.ConfigLoader
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SparkWithKafka {

  @transient val spark: SparkSession = SparkSession.builder().config(ConfigLoader.sparkConf).getOrCreate()

  import spark.implicits._

  implicit class KafkaStreamReader(reader: DataFrameReader) {

    def readFromKafka(broker: String, topic: String, startingOffsets: String = "earliest"): DataFrame = {
      val completeDS = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .load()

        completeDS.show()
   /// Value is like : 7B 22 69 64 22 3...
     val theCompleteDS = completeDS.select(col("value").as[String])
      println("asString")
      theCompleteDS.show()
       val castedTOString = completeDS.withColumn("value",col("value").cast("string"))
      // Value is : {"id":1002,"perso...
      println("Cased to String")
      castedTOString.show(false)
      val schema = spark.read.json(theCompleteDS).schema
      println(schema.toString())
     val afterReading= reader.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", startingOffsets)
        .load()
       // .withColumn("value", from_json(col("value").cast("string"), schema))
      println("afterReading")
      afterReading.show()
      afterReading.select(col("value").cast("string")).withColumn("value",from_json(col("value"),schema)).select("value.*")
    }
  }

  implicit class kafkaDFStreamWriter(dataFrame: DataFrame) {

    def writeToKafka(broker: String, topic: String): Unit = {

      val dfJSON = dataFrame.select(to_json(struct("*")) as 'value)
      dfJSON.show(false)
      dfJSON
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topic)
        .save()
    }
  }

  implicit class kafkaDSStreamWriter[A](dataSet: Dataset[A]) {

    def writeToKafka(broker: String, topic: String): Unit = {

      dataSet.select(to_json(struct("*")) as 'value)
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topic)
        .save()
    }
  }

}
