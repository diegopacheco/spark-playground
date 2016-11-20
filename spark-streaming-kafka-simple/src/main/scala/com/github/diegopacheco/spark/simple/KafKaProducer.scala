package com.github.diegopacheco.spark.simple

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object KafKaProducer extends App {
  
  val Array(brokers, topic, messagesPerSec, wordsPerMessage) = Array("localhost:9092","test","2","2")

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
        println(s"Message sent ${message} ${str}")
      }

      Thread.sleep(1000)
  }
  
}