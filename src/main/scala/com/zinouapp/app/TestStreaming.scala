package com.zinouapp.app

import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("File Count")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    val file = ssc.textFileStream("data/file1/test.txt")
    file.foreachRDD( t => {
      val test = t.flatMap( line => line.split(" "))
        .map(word => (word,1)).reduceByKey(_+_)
      test.saveAsTextFile("data/file2")
    })

    ssc.start()




  }

}
