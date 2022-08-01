package com.github.valrcs

import SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, expr, lpad, regexp_extract, regexp_replace, rpad, translate}

object Day24Exercise extends App {

  // TODO open up March 1st of 2011, CSV

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = readCSVWithView(spark, filePath)

  //Select Capitalized Description Column
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //ideally there would be even number of _______LATVIA__________ (30 total)
  //select Description column again with all occurrences of metal or wood replaced with material
  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns


  df.select(
    col("Description"),
    col("Country"),
    rpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("Country_"),
    lpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("_Country_"),
    lpad(rpad(col("Country"), 22, "_"), 30, pad="_").as("__Country__")
  ).show(10,false)

  df.select(
    col("Description"),
    col("Country"),
    rpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("Country_"),
    lpad(col("Country"), 30 - "United Kingdom".length/2, "_").as("_Country_"),
    expr("lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___")
  ).show(100,false)

  spark.sql(
    """
      |SELECT Description,
      |Country,
      |rpad(Country, 22, '_'),
      |lpad(Country, 22, '_'),
      |lpad(rpad(Country, 15+int((CHAR_LENGTH(Country))/2), '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin)
    .sample(false, fraction = 0.3)
    .show(50_000_000,false) //thank goodness we only have 1300 rows and our sample is 30 percent of that...


  val materials = Seq("wood", "metal", "ivory")
  val regexString = materials.map(_.toUpperCase).mkString("|")
  println(regexString)


  df.select(
    regexp_replace(col("Description"), regexString, "material").alias("Material_Desc"),
    col("Description"))
    .show(10,false)

  //Another task might be to replace given characters with other characters. Building this as a
  //regular expression could be tedious, so Spark also provides the translate function to replace these
  //values. This is done at the character level and will replace all instances of a character with the
  //indexed character in the replacement string

  //so we give a sort of dictionary acttually a string of values which ar to be replaced by matching character
  //in another string

  //no need for LEET to 1337 because LET to 137 does the same thing
  // L -> 1
  // E -> 3
  // I -> 1 so no problem with translating 2 characters to 1 the same
  // T -> 7

  //so basically a translation for single characters to other characters
  // resulting characters could be same
  df.select(translate(col("Description"), "LEIT", "1317"), col("Description"))
    .show(5, false)

  //so the idea is to extract some value out of particular row and basically have it in a new column
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexStringForExtraction = simpleColors.map(_.toUpperCase).mkString("(", "|", ")") //notice parenthesis
  //regex101.com

  df.select(
    regexp_extract(col("Description"), regexStringForExtraction, 1).alias("color_clean"),
    col("Description"))
    .where("CHAR_LENGTH(color_clean)>0")
    .show(10)

  //Sometimes, rather than extracting values, we simply want to check for their existence. We can do
  //this with the contains method on each column. This will return a Boolean declaring whether the
  //value you specify is in the column’s string

  //so we add a new column(with boolean whether there is black or white in description)
  //then filter by that column

  // in Scala
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(5, false)

 //SQL with instr function
  spark.sql(
    """
      |SELECT Description FROM dfTable
      |WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
      |""".stripMargin)
    .show(5, false)

  //This is trivial with just two values, but it becomes more complicated when there are values.
  //Let’s work through this in a more rigorous way and take advantage of Spark’s ability to accept a
  //dynamic number of arguments. When we convert a list of values into a set of arguments and pass
  //them into a function, we use a language feature called varargs. Using this feature, we can
  //effectively unravel an array of arbitrary length and pass it as arguments to a function. This,
  //coupled with select makes it possible for us to create arbitrary numbers of columns
  //dynamically:

  val multipleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = multipleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value //we need this to select the rest of the columns

  df.select(selectedColumns:_*). //we unroll our sequence of Columsn into multiple individual arguments
  //because select takes multiple columns one by one NOT an Sequence of columns
    show(10, false)

  //so I do not have to give all selected columns
  df.select(selectedColumns.head, selectedColumns(3), selectedColumns.last, col("Description"))
    .show(5, false)

  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

//  val numbers = Seq(1,5,6,20,5)
  //will not work on println since it does not strictly speaking support *-parameters
//  println("Something", numbers:_*) //unrolls a sequence(Array here) will print a tuple of numbers since it is equivalent TO
//  println(1,5,6,20,5)

  //so check if your method or function has * at the end of some parameter then you can unroll some sequence into those parameters
}
