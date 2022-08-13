package com.github.valrcs

import com.github.valrcs.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{asc, desc, grouping_id, max, min, sum}

object Day30JoinExerciseSolved extends App {
  //  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //  //TODO with src/resources/retail-data/customers.csv
  //  //on Customer ID in first matching Id in second

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePathCust = "src/resources/retail-data/customers.csv"

  val retailData = readDataWithView(spark, filePath, viewName = "retailData")
//  retailData.createOrReplaceTempView("retailData") //moved to above function

  val custData = readDataWithView(spark, filePathCust, viewName = "custData")
//  custData.createOrReplaceTempView("custData")

  val joinExpression = retailData.col("CustomerID") === custData.col("Id")

  val realPurchases = retailData.join(custData, joinExpression)

  realPurchases.show(10)

  realPurchases.groupBy(" LastName").sum("Quantity").show()



  spark.sql(
    """
      |SELECT * FROM retailData JOIN custData
      |ON retailData.CustomerID = custData.id
      |ORDER BY ' LastName' DESC
      |""".stripMargin)

    .show(30, false)

  //so we aggregate by sum on Quantity
  realPurchases.cube(" LastName", "InvoiceNo")
    .sum("Quantity")
    .show()

  //I will add grouping level metadata for cube
  //and we can apply various aggregation functions here
  realPurchases.cube(" LastName", "InvoiceNo")
    .agg(grouping_id(), sum("Quantity"), min("Quantity"), max("Quantity"))
    .orderBy(desc("grouping_id()")) //so level 3 is everything in the table for those 2 groups
    .show(20)

  //same as null null in cube - grouping id == 3
  realPurchases.select(sum("Quantity"), min("Quantity"), max("Quantity")).show()

  realPurchases.cube(" LastName", "InvoiceNo")
    .agg(grouping_id(), sum("Quantity"), min("Quantity"), max("Quantity"))
    .orderBy(asc("grouping_id()")) //so start with actual LastName and InvoiceNo combination
    .show(20)

  realPurchases.groupBy("StockCode")
    .pivot(" LastName")
    .sum("Quantity")
    .show(false)

  //so we should see a list of StockCode and columns for each LastName with information on purchases

  realPurchases.groupBy("StockCode", "Description") //in this case this will not generate new rows because StockCode should have unique Description
    .pivot(" LastName") //so if there are many distinct values in this column  you will have a very WIDE new dataframe
    .sum("Quantity")
    .show(false)
}
