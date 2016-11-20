package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleSparkSQLApp extends App {

  val spark = SparkSession
    .builder()
    .appName("SimpleSparkSQLApp")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  
  import spark.implicits._  
  
  case class Person(name: String, age: Long)
  val caseClassDS = Seq(Person("Andy", 32)).toDS()
  val r1 = caseClassDS.show()
  println(r1)
  
  val primitiveDS = Seq(1, 2, 3).toDS()
  val r2 = primitiveDS.map(_ + 1).collect()
  println(r2)
  
  Seq(Person("Andy", 32)).toDF().createOrReplaceTempView("people")
  
  val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 33")
  teenagersDF.map(teenager => "Name: " + teenager(0)).show()
  
  val path = "src/main/resources/people.json"
  val peopleDS = spark.read.json(path).as[Person]
  peopleDS.show()
  
  spark.stop()
  
}