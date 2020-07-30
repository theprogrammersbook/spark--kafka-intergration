package com.tpb.spark.kafka.custom

import java.util.Properties

import com.tpb.spark.kafka.common.ConfigLoader.{kafkaClientId, kafkaServer, kafkaTopic}
import org.apache.kafka.clients.consumer.ConsumerRecords

/**
 * Created by nagaraju 29/07/2020
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    //Creating propery
    val property = new Properties()
    property.put("bootstrap.servers", kafkaServer)
    property.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    property.put("value.deserializer", "com.tpb.spark.kafka.custom.CustomerDeserializer")
    property.put("group.id", kafkaClientId)
    // Crate Kafka Consumer
     val consumer  =  new org.apache.kafka.clients.consumer.KafkaConsumer[String,Customer](property)
    // Create topic
    import java.util
    val topics = new util.ArrayList[String]
    topics.add(kafkaTopic)
    consumer.subscribe(topics)
    // Read data from Kafka Server
    while(true){
      //println("Trying to read data...")
      val records:ConsumerRecords[String, Customer]  =     consumer.poll(10)
      val consumerRecordIter = records.iterator()
      while(consumerRecordIter.hasNext){
        val consumerRecord = consumerRecordIter.next()
        println(consumerRecord.value())
      }
    }
    consumer.close()

  }
}
