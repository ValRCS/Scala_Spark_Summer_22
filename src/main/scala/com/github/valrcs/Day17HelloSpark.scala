package com.github.valrcs

import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("OldSparky").master("local").getOrCreate()
  //also session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")

  val myRange = spark.range(1000).toDF("number") //create a single column dataframe (table)
  val divisibleBy5 = myRange.where("number % 5 = 0") //so similaraities with SQL and regular Scala
  divisibleBy5.show(10) //show first 10 rows

  //TODO create range of numbers 0 to 100
  //TODO filter into numbers divisible by 10
  //TODO show the results

  //TODO create range of numbers 0 to 100
  val myRange100 = spark.range(100).toDF("number")
  //TODO filter into numbers divisible by 10
  val divisibleBy10 = myRange100.where("number % 10 = 0")
  //TODO show the results
  divisibleBy10.show()

  //simplest action is to count something
  println(s"We have ${divisibleBy10.count()} numbers divisible by 10")

  //so Dataframe == Dataset[Row]
  //https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html#:~:text=Conceptually%2C%20consider%20DataFrame%20as%20an,or%20a%20class%20in%20Java.
  //as an abstraction it is just like table (except the rows might be stored across multiple computers
  //working with Dataframe we let Spark handle all the low level stuff
  //of course if you have 1 computer there is nothing to distribute :0


  //lets read a CSV

  // in Scala
  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv") //relative path to our project

  println(s"We have ${flightData2015.count()} rows of data")

  println(flightData2015.take(5).mkString(","))

  //we can adjust spark configuration for better optimization
  spark.conf.set("spark.sql.shuffle.partitions", "5") //default is 200

  //sorting is a wide transformation (meaning data needs to be shuffled across multiple partitions)

  println(flightData2015.sort("count").take(10).mkString(","))

  //we can explain our queries - that is we let Spark tell us how it is going to peform the action
  flightData2015.sort("count").explain()

  readLine("Enter anything to stop spark")
  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}
