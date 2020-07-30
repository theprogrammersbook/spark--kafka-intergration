package com.tpb.spark.kafka.stream

import java.util.Properties

import com.tpb.spark.kafka.common.ConfigLoader.{kafkaClientId, kafkaServer, kafkaTopic}

object KafkaProducerForStreaming {

  def main(args: Array[String]): Unit = {

    val kafkaProperty = new Properties()
    kafkaProperty.put("bootstrap.servers", kafkaServer)
    kafkaProperty.put("client.id", kafkaClientId)
    kafkaProperty.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperty.put("value.serializer", "com.tpb.spark.kafka.stream.CustomCaseClassSerializer")
    while (true) {
      val caseClassList = 1 to 25 map { index => CustomCaseClass(index, "Custom Name " + index) }
      val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[String, CustomCaseClass](kafkaProperty)

      caseClassList foreach { customer =>
        println(customer.toString)
        val producerRecord = new org.apache.kafka.clients.producer.ProducerRecord[String, CustomCaseClass](kafkaTopic+"-streaming", "1", customer)
        kafkaProducer.send(producerRecord)
      }
      kafkaProducer.close()
    }

  }
}
