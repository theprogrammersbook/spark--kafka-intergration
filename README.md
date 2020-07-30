# Spark-Kafka-Stream-Example
# Kafka Custom Serializable and Deserializer

This repo contains the Example of Spark using Apache Kafka.

##### We Are Using :- <br />
        Spark Version :- 2.4.6
        Scala Version :- 2.11.11
        Lift Json Version :- 2.6.2

We are Feeding Case class Object to Apache Kafka via Kafka Producer and Fetching the same via KafkaConsumer
we can test these behaviour on the package : `custom`
### Flow :-<br />
        Customer(1, "Sample Name") -> Apache Kafka [Producer]
        Apache Kafka [Consumer]  -> Customer(1, "Sample Name") -> Show.
### How to run th above.
- Run the KafkaProducer.scala 
- Run the KafkaConsumer.scala
------------------------------------------------------------------------------------
### When we test with spark sql with kafka then we are inserting persons data and retrieving data.
- KafkaProdConsSparkSql.scala  -- Will insert persons data to topic person and retrives data.
- KafkaSparkSqlStreaming.scala -- Reads the data from topic(persons) as batch of stream.

We are Feeding Case class Object to Apache Kafka via Kafka Producer and Fetching the same via Spark Streaming and printing the Case class Object in String Form.

### Flow :-<br />
        CustomCaseClass(1, "Sample Name") -> Apache Kafka [Producer]
        Apache Kafka [Consumer] -> Spark Streaming -> CustomCaseClass(1, "Sample Name") -> Show.
### How to run the streaming applicaiton.

- Step1 : Run KafkaProducerForStreaming.scala to insert data to tweet-streaming topic.
- Step2 : Run KafkaConsumerSparkStreaming.scala to check the streaming of data from tweet-streaming topic.

