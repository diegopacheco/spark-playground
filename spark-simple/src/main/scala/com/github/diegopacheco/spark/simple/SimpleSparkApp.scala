package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleSparkApp extends App {
  
  val conf = new SparkConf().setAppName("SimpleAsparkApp").setMaster("local")
  val sc = new SparkContext(conf)
  
  val fileLocation = "src/main/resources/data.txt"
  
  val lines = sc.textFile(fileLocation)
  val lineLengths = lines.map(s => s.length)
  val totalLength = lineLengths.reduce((a, b) => a + b)
  println(totalLength)
  
  sc.stop()
  
}