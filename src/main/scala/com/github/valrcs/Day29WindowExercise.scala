package com.github.valrcs

import com.github.valrcs.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, max, min, rank, to_date}

object Day29WindowExercise extends App {
  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/all/*.csv"

  val df = readDataWithView(spark, filePath)

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY") //TODO see if you can parse correctly without this
  //https://en.wiktionary.org/wiki/Chesterton%27s_fence#:~:text=Chesterton's%20fence%20(uncountable),state%20of%20affairs%20is%20understood.

  //compare to the other side of coin
  //https://en.wikipedia.org/wiki/Cargo_cult_programming

  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm"))
  dfWithDate.createOrReplaceTempView("dfWithDateView")

//  val windowSpec = Window
//    .partitionBy("CustomerId", "date")
//    .orderBy(col("Quantity").desc)
//    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
//
//  val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
//
//  val purchaseDenseRank = dense_rank().over(windowSpec)
//  val purchaseRank = rank().over(windowSpec)
//
//  dfWithDate.where("CustomerId IS NOT NULL")
//    .orderBy("date", "CustomerId")
//    .select(
//      col("CustomerId"),
//      col("date"),
//      col("Quantity"),
//      purchaseRank.alias("quantityRank"),
//      purchaseDenseRank.alias("quantityDenseRank"),
//      maxPurchaseQuantity.alias("maxPurchaseQuantity"))
//    .show(20, false)


  //TODO create WindowSpec which partitions by StockCode and date, ordered by Price
  //with rows unbounded preceding and current row
  //create max, min, dense rank and rank for the price oer the newly created WindowSpec

  // show top 40 results ordered un descending order by StockCode ad Price
  // show max, min, dense rank and rank for every row as well using new column

  val windowSpec2 = Window
    .partitionBy("StockCode", "date")
    .orderBy(col("UnitPrice").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)



  val maxPrice = max(col("UnitPrice")).over(windowSpec2)
  val minPrice = min(col("UnitPrice")).over(windowSpec2)

  val priceDenseRank = dense_rank().over(windowSpec2)
  val priceRank = rank().over(windowSpec2)

  dfWithDate.where("StockCode IS NOT NULL")
    .orderBy(desc("StockCode"), desc("UnitPrice"))
    .select(
      col("date"),
      col("StockCode"),
      col("Description"),
      col("UnitPrice"),
      priceRank.alias("priceRank"),
      priceDenseRank.alias("priceDenseRank"),
      maxPrice.alias("maxPrice"),
      minPrice.alias("minPrice"))
    .show(40, false)

  spark.sql(
    """
      |SELECT date, StockCode, UnitPrice,
      |rank(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as rank,
      |dense_rank(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as dRank,
      |max(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |UNBOUNDED FOLLOWING) as maxPrice,
      |min(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |UNBOUNDED FOLLOWING) as minPrice,
      |count(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |UNBOUNDED FOLLOWING) as countPrice
      |FROM dfWithDateView WHERE StockCode IS NOT NULL
      |ORDER BY StockCode DESC, UnitPrice DESC
      |""".stripMargin)
    .show(40, false)
}
