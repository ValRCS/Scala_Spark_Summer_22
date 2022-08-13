package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark


object Day30Joins extends App {
  println("Ch8: Joins")
  //https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html
  //all joins latest syntax

  val spark = getSpark("Sparky")

  //Join Expressions
  //A join brings together two sets of data, the left and the right, by comparing the value of one or
  //more keys of the left and right and evaluating the result of a join expression that determines
  //whether Spark should bring together the left set of data with the right set of data. The most
  //common join expression, an equi-join, compares whether the specified keys in your left and
  //right datasets are equal. If they are equal, Spark will combine the left and right datasets. The
  //opposite is true for keys that do not match; Spark discards the rows that do not have matching
  //keys. Spark also allows for much more sophsticated join policies in addition to equi-joins. We
  //can even use complex types and perform something like checking whether a key exists within an
  //array when you perform a join.

  //Join Types
  //Whereas the join expression determines whether two rows should join, the join type determines
  //what should be in the result set. There are a variety of different join types available in Spark for
  //you to use:
  //Inner joins (keep rows with keys that exist in the left and right datasets)
  //Outer joins (keep rows with keys in either the left or right datasets)
  //Left outer joins (keep rows with keys in the left dataset)
  //Right outer joins (keep rows with keys in the right dataset)
  //Left semi joins (keep the rows in the left, and only the left, dataset where the key
  //appears in the right dataset)
  //Left anti joins (keep the rows in the left, and only the left, dataset where they do not
  //appear in the right dataset)
  //Natural joins (perform a join by implicitly matching the columns between the two
  //datasets with the same names)
  //Cross (or Cartesian) joins (match every row in the left dataset with every row in the
  //right dataset)
  //If you have ever interacted with a relational database system, or even an Excel spreadsheet, the
  //concept of joining different datasets together should not be too abstract. Let’s move on to
  //showing examples of each join type. This will make it easy to understand exactly how you can
  //apply these to your own problems. To do this, let’s create some simple datasets that we can use
  //in our examples
  //some simple Datasets
  import spark.implicits._ //implicits will let us use toDF on simple sequence
  //regular Sequence does not have toDF method that why we had to use implicits from spark

  // in Scala
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (3, "Valdis Saulespurens", 2, Seq(100,250)),
    (4, "Victoria Beckham", 42, Seq(100,250)), //42 does not correspond to any school at a time in our graduatePrograms table
    (5, "Harry Potter", 77, Seq(100,250,500,1000)), //42 does not correspond to any school at a time in our graduatePrograms table
  )
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"),
    (3, "Masters", "EECS", "University of Latvia"),
    (4, "Masters", "EECS", "Vilnius University"),
  )
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President", 500_000),
    (250, "PMC Member", 15_000),
    (100, "Contributor", 10))
    .toDF("id", "status", "salary")

  person.show()
  graduateProgram.show()
  sparkStatus.show()


  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  //Inner Joins
  //Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together)
  //only the rows that evaluate to true. In the following example, we join the graduateProgram
  //DataFrame with the person DataFrame to create a new DataFrame:

  // in Scala - notice the triple ===
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  //Inner joins are the default join, so we just need to specify our left DataFrame and join the right in
  //the JOIN expression:
  person.join(graduateProgram, joinExpression).show()

  //same in spark sql
  spark.sql(
    """
      |SELECT * FROM person JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL

  //We can also specify this explicitly by passing in a third parameter, the joinType:

  //again no need to pass inner since it is the default
  person.join(graduateProgram, joinExpression, joinType = "inner").show()

  //Outer Joins
  //Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins
  //together) the rows that evaluate to true or false. If there is no equivalent row in either the left or
  //right DataFrame, Spark will insert null

  println("FULL OUTER JOIN")

  person.join(graduateProgram, joinExpression, "outer").show()

  spark.sql(
    """
      |SELECT * FROM person
      |FULL OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //so we should see some null values in an FULL OUTER JOIN

  //Left Outer Joins
  //Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
  //the left DataFrame as well as any rows in the right DataFrame that have a match in the left
  //DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null

  println("LEFT OUTER JOINS")
  person.join(graduateProgram, joinExpression, "left_outer").show()
  spark.sql(
    """
      |SELECT * FROM person
      |LEFT OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Right Outer Joins
  //Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows
  //from the right DataFrame as well as any rows in the left DataFrame that have a match in the right
  //DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

  println("RIGHT OUTER JOIN")

  person.join(graduateProgram, joinExpression, "right_outer").show()
  spark.sql(
    """
      |SELECT * FROM person
      |RIGHT OUTER JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()


  //Left Semi Joins
  //Semi joins are a bit of a departure from the other joins. They do not actually include any values
  //from the right DataFrame. They only compare values to see if the value exists in the second
  //DataFrame. If the value does exist, those rows will be kept in the result, even if there are
  //duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame, as
  //opposed to the function of a conventional join:

  println("LEFT SEMI JOIN")
  //notice we are using the same joinExpression but we start with graduateProgram here
  graduateProgram.join(person, joinExpression, "left_semi").show() //should show only programs with graduates
  person.join(graduateProgram, joinExpression, "left_semi").show() //should show only persons who attended a known university


  //we create a new dataFrame by using Union of two dataframes
  // in Scala
  //in this case the id is a duplicate of previous one
  //often you do not want this :)
  val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")

  gradProgram2.join(person, joinExpression, "left_semi").show()

  spark.sql(
    """
      |SELECT * FROM gradProgram2 LEFT SEMI JOIN person
      |ON gradProgram2.id = person.graduate_program
      |""".stripMargin)
    .show()

  // more on differences on INNER JOIN and LEFT SEMI JOIN
  //https://stackoverflow.com/questions/21738784/difference-between-inner-join-and-left-semi-join

  //Left Anti Joins
  //Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually
  //include any values from the right DataFrame. They only compare values to see if the value exists
  //in the second DataFrame. However, rather than keeping the values that exist in the second
  //DataFrame, they keep only the values that do not have a corresponding key in the second
  //DataFrame. Think of anti joins as a NOT IN SQL-style filter:

  println("LEFT ANTI JOIN")

  graduateProgram.join(person, joinExpression, "left_anti").show()

  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |LEFT ANTI JOIN person
      |ON graduateProgram.id = person.graduate_program
      |""".stripMargin)
    .show()

  //so we should expect to see University of Latvia and Vilnius University since they do not have any known students here

  //similarly we should expect to see students without a matching university program if our left table is persons
  person.join(graduateProgram, joinExpression, "left_anti").show()

}
