package com.github.valrcs

import com.github.valrcs.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

object Day37CumulativeSum extends App {
  val spark = getSpark("SparkY")

  val defaultSrc = "src/resources/csv/fruits.csv"
  //so our src will either be default file  or the first argument supplied by user
  val src = if (args.length >= 1) args(0) else defaultSrc

  println(s"My Src file will be $src")

  val df = readDataWithView(spark, src)
    .withColumn("id",monotonically_increasing_id)
    .withColumn("total", expr("quantity * price"))

  //I have to create the view again since original view does not have total column
  df.createOrReplaceTempView("dfTable")
  df.show()

  val sumDF = spark.sql(
    """
      |SELECT *, SUM(total) OVER
      |(ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as CSUM,
      |SUM(total) OVER
      |(PARTITION BY fruit
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as SUMFRUIT
      |FROM dfTable
      |""".stripMargin)
  //so our sum is over default ordering an no partitions
  sumDF.show(false)

  //TODO do the same using spark function, also check ROUND function

}
