package com.tpb.spark.kafka.stream

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import org.apache.kafka.common.serialization.Serializer

/**
  * Created by nagaraju on 29/07/2020.
  */
class CustomCaseClassSerializer extends Serializer[CustomCaseClass] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def serialize(topic: String, data: CustomCaseClass): Array[Byte] = {

    val byteOut = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(byteOut)
    objOut.writeObject(data)
    objOut.close()
    byteOut.close()
    byteOut.toByteArray
  }

  override def close(): Unit = {

  }
}
