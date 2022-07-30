package com.github.valrcs

import org.apache.spark.sql.functions.col

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
  df.createOrReplaceTempView("dfTable")

  //We mentioned that you can specify Boolean expressions with multiple parts when you use and
  //or or. In Spark, you should always chain together AND filters as a sequential filter

  //The reason for this is that even if Boolean statements are expressed serially (one after the other),
  //Spark will flatten all of these filters into one statement and perform the filter at the same time,
  //creating the and statement for us. Although you can specify your statements explicitly by using
  //and if you like, they’re often easier to understand and to read if you specify them serially

  //OR or
  //statements need to be specified in the same statement:

  // in Scala
  val priceFilter = col("UnitPrice") > 600  //so Filter is a Column type
  val descriptionFilter = col("Description").contains("POSTAGE") //again Column type

  //here we use prexisting order
  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter))
    .show()

  //so messier with SQL
  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode IN ('DOT') AND (UnitPrice > 600 OR Description LIKE '%POSTAGE%')")
    .show()

//  /
  // Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also just
  //specify a Boolean column:
  //in Scala
  val DOTCodeFilter = col("StockCode") === "DOT"

  //so we add a new Boolean column which shows whether StockCode is named DOT
  df.withColumn("stockCodeDOT", DOTCodeFilter).show(10)
  df.withColumn("stockCodeDOT", DOTCodeFilter)
    .where("stockCodeDOT") //so filters by existance of truth in this column
    .show()

  //so we create a boolean column using 3 filters cond1 && (cond2 || cond3)
  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
    .where("isExpensive") //immediately filter by that column
    .select("unitPrice", "isExpensive") //we only show specific columns
    .show(5)


}
