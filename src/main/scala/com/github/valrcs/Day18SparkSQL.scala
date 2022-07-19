package com.github.valrcs

import org.apache.spark.sql.SparkSession

object Day18SparkSQL extends App {
  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
//  spark.sparkContext.setLogLevel("WARN")
  println(s"Session started on Spark version ${spark.version}")
}
