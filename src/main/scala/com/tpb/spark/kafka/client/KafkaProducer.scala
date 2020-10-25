package com.tpb.spark.kafka.custom

import java.util.Properties

import com.tpb.spark.kafka.common.ConfigLoader.{kafkaClientId, kafkaServer, kafkaTopic}

object KafkaProducer {

  /*
  bin/kafka-topics --bootstrap-server localhost:9092 --create --topic my_topic_name \
 --partitions 20 --replication-factor 3 --config x=y
   */

  def main(args: Array[String]): Unit = {
   //Creating properties for the kafka
    val kafkaProperty = new Properties()
    kafkaProperty.put("bootstrap.servers", kafkaServer)
    kafkaProperty.put("client.id", kafkaClientId)
    kafkaProperty.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperty.put("value.serializer", "com.tpb.spark.kafka.custom.CustomerSerializer")
    // untill the user manually kills the producer , it will run continuously
    while (true) {
      // Created Sequence of Customer object.
     val check = 1 to 10 seq
      val caseClassList = 1 to 25 map { index => Customer(index, "Custom Name " + index) }
      val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[String, Customer](kafkaProperty)

      caseClassList foreach { customer =>
        println(customer.toString)
        val producerRecord = new org.apache.kafka.clients.producer.ProducerRecord[String, Customer](kafkaTopic, "1", customer)
        kafkaProducer.send(producerRecord)
      }
      kafkaProducer.close()
    }

  }
}
