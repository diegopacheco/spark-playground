package com.github.diegopacheco.spark.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object SimpleSparkGraphXApp extends App {
  
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt")
    val ranks = graph.pageRank(0.0001).vertices
    val users = sc.textFile("src/main/resources/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    println(ranksByUsername.collect().sortBy(_._2)(Ordering[Double].reverse).mkString("\n"))

    spark.stop()
  
  
}