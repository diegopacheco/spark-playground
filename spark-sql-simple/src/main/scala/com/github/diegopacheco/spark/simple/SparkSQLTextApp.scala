package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

object SparkSQLTextApp extends App {
  
    val spark = SparkSession
    .builder()
    .appName("SparkSQLTextApp")
    .config("spark.some.config.option", "some-value")
    .master("local")
    .getOrCreate()
  
  import spark.implicits._
  
  case class Person(name: String, age: Long)
  
  val peopleDF = spark.sparkContext
    .textFile("src/main/resources/people.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()
  peopleDF.createOrReplaceTempView("people")

  val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
  teenagersDF.show()
  
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  
}