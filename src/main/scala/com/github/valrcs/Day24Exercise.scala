package com.github.valrcs

import SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, lpad, regexp_extract, regexp_replace, rpad, translate}

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

}
