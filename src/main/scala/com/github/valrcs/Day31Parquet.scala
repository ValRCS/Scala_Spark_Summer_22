package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark

object Day31Parquet extends App {
  val spark = getSpark("Sparky")

  val df = spark.read.format("parquet")
//    .load("src/resources/flight-data/parquet/2010-summary.parquet")
    //version mismatch generates warnings - creator metadata not preserved
    //https://stackoverflow.com/questions/42320157/warnings-trying-to-read-spark-1-6-x-parquet-into-spark-2-x
    .load("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

  df.show()

//  df.write.format("parquet").mode("overwrite")
//    .save("src/resources/flight-data/parquet/2010-summary_fixed.parquet")

}
