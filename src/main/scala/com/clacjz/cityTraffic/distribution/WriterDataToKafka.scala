package com.clacjz.cityTraffic.distribution

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object WriterDataToKafka {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)

    val producer= new KafkaProducer[String,String](props)
    var in = new BufferedReader(new InputStreamReader(new FileInputStream("删去")))
    var line =in.readLine()
    while (line!=null){
      val record:ProducerRecord[String,String] = new ProducerRecord[String,String]("traffic",null,line)
      producer.send(record)
      line =in.readLine()
    }
    in.close()
    producer.close()
  }
}
