package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark
import org.apache.spark.sql.functions.expr

object Day36CreateRegressionData extends App {
  val spark = getSpark("Sparky")

  val dst = "src/resources/csv/range100"

  //no seed is set so each time should be different random noise

  val df = spark
    .range(100)
    .toDF()
    .withColumnRenamed("id", "x")
    .withColumn("y", expr("round(x*4+5+rand()-0.5, 3)")) //so our linear formula has some noise
  //so our formula ix f(x) = 4x+5+some random noise from -0.5 to 0.5

  df.show(10, false)

  df.write
    .format("csv")
    .option("path", dst)
    .option("header", true)
    .mode("overwrite")
    .save



}
