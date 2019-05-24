package consumer

import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object ResultConsumer extends App {

  if (args.length != 2) {
    println("Please specify Kafka broker URL and listening topic")
  }
  val TOPIC = args(1)

  import java.util.Properties

  val props = new Properties()
  props.put("bootstrap.servers", args(0))
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "log_stat")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records = consumer.poll(1000)
    for (record <- records.asScala) {
      println(record.value())
    }
  }
}