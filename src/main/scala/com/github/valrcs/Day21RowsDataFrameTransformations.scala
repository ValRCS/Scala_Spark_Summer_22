package com.github.valrcs

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType }

object Day21RowsDataFrameTransformations extends App {
  println("Chapter 5. Basic Structured Operations - Rows, DataFrame Transformations ")
  val spark = SparkUtil.getSpark("BasicSpark")

  //Creating Rows
  //You can create rows by manually instantiating a Row object with the values that belong in each
  //column. It’s important to note that only DataFrames have schemas. Rows themselves do not have
  //schemas. This means that if you create a Row manually, you must specify the values in the same
  //order as the schema of the DataFrame to which they might be appended

  val myRow = Row("Hello Sparky!", null, 555, false, 3.1415926)

  //we get access to individual members of Row, quite similar to how we would get access to individual members of an Array
  println(myRow(0))
  println(myRow(0).asInstanceOf[String]) // String
  println(myRow.getString(0)) // String
  val myGreeting = myRow.getString(0) //myGreeting is just normal Scala string
  println(myGreeting)
  println(myRow.getInt(2)) // Int
  val myDouble = myRow.getInt(2).toDouble //we get and cast our Int as Double from Scala not Spark
  println(myDouble)
  val myPi = myRow.getDouble(4) //so 5th element
  println(myPi)

  //so we can in fact print schema property for a single row
  //but we do not have printSchema method
  println(myRow.schema) //so no schema unless Row comes from a DataFrame

  //DataFrame Transformations

  //When working with individual DataFrames there are some fundamental objectives.
  //These break down into several core operations

  //We can add rows or columns
  //We can remove rows or columns
  //We can transform a row into a column (or vice versa) - transposition
  //We can change the order of rows based on the values in column

  //the most common being those
  //that take one column, change it row by row, and then return our results

  val flightPath = "src/resources/flight-data/json/2015-summary.json"

  //so automatic detection of schema
  val df = spark.read.format("json")
    .load(flightPath)

  df.createOrReplaceTempView("dfTable") //view (virtual Table) needed to make SQL queries

  df.show(5)

  //We can also create DataFrames on the fly by taking a set of rows and converting them to a
  //DataFrame.
  //we will need to define a schema
  val myManualSchema = new StructType(Array(
    StructField("some", StringType, true), //so true refers to this field/column being nullable namely could have null
    StructField("col", StringType, true),
    StructField("names", LongType, false))) //so names have to be present it is not nullable - ie required

  val myRows = Seq(Row("Hello", null, 1L), //we need to specify 1L because 1 by itself is an integer
    Row("Sparky", "some string", 151L),
    Row("Valdis", "my data", 9000L),
    Row(null, "my data", 151L)
  )

  //I do need to drop down to lower level RDD structure
  val myRDD = spark.sparkContext.parallelize(myRows) //you could add multiple partitions(numSlices) here which is silly when you only have 4 rows o data
  val myDf = spark.createDataFrame(myRDD, myManualSchema) //so i pass RDD and schema and I get DataFrame
  myDf.show()

  //there is a slightly hacky way to do conversions of Rows to DataFrames
  //NOTE
  //In Scala, we can also take advantage of Spark’s implicits in the console (and if you import them in
  //your JAR code) by running toDF on a Seq type. This does not play well with null types, so it’s not
  //necessarily recommended for production use cases.
  // in Scala
  //this requires implicits which are slowly going away in future versions
//  val alsoDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

}

