package com.github.valrcs

import com.github.valrcs.SparkUtil.{getSpark, readDataWithView}

object Day30JoinExerciseSolved extends App {
  //  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //  //TODO with src/resources/retail-data/customers.csv
  //  //on Customer ID in first matching Id in second

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePathCust = "src/resources/retail-data/customers.csv"

  val retailData = readDataWithView(spark, filePath, viewName = "retailData")
//  retailData.createOrReplaceTempView("retailData") //moved to above function

  val custData = readDataWithView(spark, filePathCust)
  custData.createOrReplaceTempView("custData")

  val joinExpression = retailData.col("CustomerID") === custData.col("id")

  retailData.join(custData, joinExpression).show(30, false)

  spark.sql(
    """
      |SELECT * FROM retailData JOIN custData
      |ON retailData.CustomerID = custData.id
      |ORDER BY ' LastName' DESC
      |""".stripMargin)

    .show(30, false)
}
