package producer

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

object LogGenerator extends App {

  if (args.length != 3) {
    println("Please specify Kafka broker URL, topic and host name")
  }

  val TOPIC = args(1)

  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))
  props.put("group.id", "log_producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "LogProducer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)

  val rnd = new Random()
  val logLevels = List("TRACE", "DEBUG", "INFO", "WARN", "ERROR")
  val host = args(2)

  for (i <- (0 to 1000000) ) {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datetimeNow = formatter.format(new Date)
    val level = logLevels(rnd.nextInt(logLevels.size))
    val text = s"some message of level $level"
    val jsonMessage = s"""{"timestamp":"$datetimeNow", "host":"$host", "level":"$level", "text":"$text"}"""
    println(jsonMessage)
    kafkaProducer.send(new ProducerRecord(TOPIC, jsonMessage))
    Thread.sleep(rnd.nextInt(400))
  }
}
