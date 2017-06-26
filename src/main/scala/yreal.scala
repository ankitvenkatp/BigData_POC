/**
  * Created by ankit  on 6/19/2017.
  */

import java.util.Properties

import sys.process._
import java.net.URL
import java.io.File

import org.apache.kafka.clients.producer._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import yhist.hv

object yreal extends App {

  def fileDownloader(url: String) = {
    new URL(url) #> new File("/home/cloudera/Documents/stock/data.csv") !!
  }



  val conf = new SparkConf().setAppName("yahooreal")
  val sc = new SparkContext(conf)
  val hv = new HiveContext(sc)
  val file = sc.textFile("file:////home/cloudera/Documents/stock/data.csv")

  val  props = new Properties()
  props.put("bootstrap.servers", "0.0.0.0:9092")
  props.put("batch.size", "2")
//  props.put("linger.ms", "120000")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val topic = "ystock"

  hv.sql("USE default")
  hv.sql("CREATE TABLE IF NOT EXISTS abdeReal (r_symbol String, r_name String,r_date String,r_lasttrade float,r_change String,r_daylow float, r_dayhigh float,r_yearlow float, r_yearhigh float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

  for(i<- 1 to 6) {
    fileDownloader("http://download.finance.yahoo.com/d/quotes.csv?s=adbe&f=snd1l1p2ghjk")
    var value = file.collect()(0)
    val record = new ProducerRecord(topic,i.toString, value)
    producer.send(record)
    hv.sql(s"insert into table abdeReal select t.* from (select $value)t")
    Thread.sleep(60000)
  }
  producer.close()

}
