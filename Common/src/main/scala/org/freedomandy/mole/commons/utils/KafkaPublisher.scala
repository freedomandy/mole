package org.freedomandy.mole.commons.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author Andy Huang on 2018/6/18
  */
class KafkaPublisher(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: String, value: String): Unit =
    producer.send(new ProducerRecord(topic, key, value))
}

object KafkaPublisher {
  def apply(host: String, port: String = "9092"): KafkaPublisher = {
    val f = () => {
      println("Initialize connection..")

      val  props = new Properties()

      props.put("bootstrap.servers", s"$host:$port")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }

    new KafkaPublisher(f)
  }
}
