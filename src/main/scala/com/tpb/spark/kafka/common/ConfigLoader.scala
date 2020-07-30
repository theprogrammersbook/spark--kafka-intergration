package com.tpb.spark.kafka.common

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf

/**
  * Created by Nagaraju on 29/07/2020.
  */
object ConfigLoader {

  val configFactory: Config = ConfigFactory.load()

  val sparkAppName: String = configFactory.getString("spark.app.name")
  val sparkMaster: String = configFactory.getString("spark.ip")
  val sparkCores: String = configFactory.getString("spark.cores")
  val sparkMemory: String = configFactory.getString("spark.executor.memory")

  val kafkaServer: String = configFactory.getString("kafka.server")
  val kafkaTopic: String = configFactory.getString("kafka.topic")
  val kafkaClientId: String = configFactory.getString("kafka.clientId")

  val zookeeperUrl: String = configFactory.getString("zookeeper.server")

  val sparkConf: SparkConf = new SparkConf()
    .setAppName(sparkAppName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkMemory)
    .set("spark.executor.core", sparkCores)
}
