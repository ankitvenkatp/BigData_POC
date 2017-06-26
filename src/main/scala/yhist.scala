/**
  * Created by ankit on 6/16/2017.
  */
import sys.process._
import java.net.URL
import java.io.File

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object yhist extends App {

  def fileDownloader(url: String) = {
    new URL(url) #> new File("/home/cloudera/Documents/stock/data.csv") !!
  }

  fileDownloader("http://www.google.com/finance/historical?q=NASDAQ:ADBE&startdate=Jan+01%2C+2007&enddate=Jun+16%2C+2017&output=csv")
  val conf = new SparkConf().setAppName("teraSpark")
  val sc = new SparkContext(conf)
  val hv = new HiveContext(sc)
  val file = sc.textFile("file:////home/cloudera/Documents/stock/data.csv")
  file.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/stock/data.csv")

  hv.sql("USE default")
  hv.sql("CREATE TABLE abde (i_date String, i_open float, i_high float, i_low float,i_close float,i_volume float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
  hv.sql("LOAD DATA INPATH '/user/cloudera/stock/data.csv' OVERWRITE INTO TABLE abde")
}
