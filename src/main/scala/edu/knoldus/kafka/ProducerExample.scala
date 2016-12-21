package edu.knoldus.kafka

import java.util.{Date, Properties}
import SimpleProducer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SimpleProducer{

  private var producer: KafkaProducer[String, String] = _

  def main(args: Array[String]) {
    val argsCount = args.length
    if (argsCount == 0 || argsCount == 1)
      throw new IllegalArgumentException(
        "Provide topic name and Message count as arguments")

    val topic = args(0)
    val count = args(1)

    val messageCount = java.lang.Integer.parseInt(count)
    println("Topic Name - " + topic)
    println("Message Count - " + messageCount)

    val simpleProducer = new SimpleProducer()
    simpleProducer.publishMessage(topic, messageCount)
  }
}

class SimpleProducer {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  //This specifies the serializer class for keys
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // 1 means the producer receives an acknowledgment once the lead replica
  // has received the data. This option provides better durability as the
  // client waits until the server acknowledges the request as successful.
  props.put("request.required.acks", "1")
  producer = new KafkaProducer(props)

  private def publishMessage(topic: String, messageCount: Int) {
    for (mCount <- 0 until messageCount) {
      val runtime = new Date().toString
      val msg = "Message Publishing Time - " + runtime
      println(msg)
      // Create a message
      val data = new ProducerRecord[String, String](topic, msg)
      // Publish the message
      producer.send(data)
    }
    // Close producer connection with broker.
    producer.close()
  }
}