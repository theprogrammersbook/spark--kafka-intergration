package com.tpb.spark.kafka.custom

import java.io.{ByteArrayInputStream, ObjectInputStream}

import org.apache.kafka.common.serialization.Deserializer

class CustomerDeserializer() extends Deserializer[Customer] {

  override def deserialize(topic: String, bytes: Array[Byte]): Customer = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[Customer]
    byteIn.close()
    objIn.close()
    obj
  }
}
