package com.github.valrcs

object Day23WorkingWithDataTypes extends App {
  println("Ch6: Working with Different Types\nof Data - Part 2")
  val spark = SparkUtil.getSpark("BasicSpark")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //we let Spark determine schema
    .load(filePath)

  df.printSchema()


}
