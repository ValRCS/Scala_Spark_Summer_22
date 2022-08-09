package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark
import com.github.valrcs.Util.myRound
import org.apache.spark.sql.functions.{col, udf}

object Day27ExerciseUDF extends App {
  println("Ch6: UDFs - User Defined Functions")
  val spark = getSpark("Sparky")


  //TODO create a UDF which converts Fahrenheit to Celsius

//  def celsius(fahrenheit:Int):Double =( 5 *(fahrenheit - 32.0)) / 9.0
def tempC(tempF: Double):Double = myRound((tempF-32)*5/9 , 2)
  //TODO Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want

  val dfTemp = spark.range(-40,120).toDF("temperatureF")
  dfTemp.show()
  //TODO register your UDF function

  val celsiusUDF = udf(tempC(_:Double):Double)
  //TODO use your UDF to create temperatureC column with the actual conversion
  dfTemp.withColumn("temperatureC",celsiusUDF(col("temperatureF"))).show(5)
  //TODO show both columns starting with F temperature at 90 and ending at 110( both included)
  val dfTable = dfTemp
    .withColumn("temperatureC",celsiusUDF(col("temperatureF")))
    .where(col("temperatureF") >= 90 && col("temperatureF") <= 110)
  dfTable.show()

}
