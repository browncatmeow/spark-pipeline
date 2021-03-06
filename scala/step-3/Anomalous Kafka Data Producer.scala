// Databricks notebook source exported at Sat, 26 Dec 2015 09:36:02 UTC
// MAGIC %md # **Kafka Sensor Data Producer**
// MAGIC 
// MAGIC Use this to generate anomalous data for running Step 3: Monitoring Your Service

// COMMAND ----------

// === Configurations for Kafka ===
val kafkaTopic = "YOUR_KAFKA_TOPICS"    // comma separated list of topics
val kafkaBrokers = "YOUR_KAFKA_BROKERS"   // comma separated list of broker:host

// === Configurations of amount of data to produce ===
val numSensors = 2
val numSecondsToSend = 1000


// COMMAND ----------

import java.util.HashMap
import scala.util.Random
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

// Verify that the Kinesis settings have been set
require(!kafkaTopic.contains("YOUR"), "Kafka topic have not been set")
require(!kafkaBrokers.contains("YOUR"), "Kafka brokers have not been set")


// COMMAND ----------

// Normal data had mu = 10.0 and sigma = 1.0
val mu = 12.0
val sigma = 0.1

// COMMAND ----------

val props = new HashMap[String, Object]()
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
  "org.apache.kafka.common.serialization.StringSerializer")
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
  "org.apache.kafka.common.serialization.StringSerializer")

val producer = new KafkaProducer[String, String](props)

// Generate and send the data
for (round <- 1 to numSecondsToSend) {
  for (sensorNum <- 1 to numSensors) {
    val data = (mu + math.abs(Random.nextGaussian()) / sigma).toString
    val message = new ProducerRecord[String, String](kafkaTopic, sensorNum.toString, data)
    producer.send(message)
  }
  Thread.sleep(1000) // Sleep for a second
}

