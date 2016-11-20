package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleSparkApp extends App {
  
  val conf = new SparkConf().setAppName("SimpleAsparkApp").setMaster("local")
  val sc = new SparkContext(conf)
  
  val fileLocation = "src/main/resources/data.txt"
  
  val lines = sc.textFile(fileLocation).cache()
  val lineLengths = lines.map(s => s.length)
  val totalLength = lineLengths.reduce((a, b) => a + b)
  println(totalLength)
  
  val numAs = lines.filter(line => line.contains("A")).count()
  val numBs = lines.filter(line => line.contains("B")).count()
  println(s"TOTAL = A -> ${numAs} B -> ${numBs}")
 
  val wordCounts = lines.flatMap(line => line.split(" ")).
                         filter(!_.headOption.exists(_.isDigit)).
                         map(word => (word, 1)).
                         reduceByKey((a, b) => a + b)
  
  wordCounts.collect().sortBy(_._2)(Ordering[Int].reverse).foreach { 
     case (w,c) => println(s"Word: [${w}] Count: ${c}")
  }
  
  sc.stop()
  
}