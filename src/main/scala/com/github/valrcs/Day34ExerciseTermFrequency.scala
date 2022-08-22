package com.github.valrcs

import com.github.valrcs.SparkUtil.getSpark
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}

object Day34ExerciseTermFrequency extends App {

  val spark = getSpark("Sparky")

  //TODO using tokenized alice - from weekend exercise

  val path = "src/resources/text/Alice.txt"
  val df = spark.read.textFile(path).withColumnRenamed("value", "text")
  df.cache()

  val tkn = new Tokenizer().setInputCol("text").setOutputCol("words")
  val dfWithWords = tkn.transform(df.select("text"))

  //TODO remove english stopwords

  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  println("First 10 stopwords")
  println(englishStopWords.take(10).mkString(","))

  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords)
    .setInputCol("words")
    .setOutputCol("noStopWords")
  stops.transform(dfWithWords).show(20, false)

  //Create a CountVectorized of words/tokens/terms that occur in at least 3 documents(here that means rows)
  //the terms have to occur at least 2 times in each row
  //TODO show first 30 rows of data

  val cv = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("countVec")
    .setVocabSize(500)
//    .setMinTF(2) //so term has to appear at least twice
    .setMinTF(1) //so term has to appear at least once
    .setMinDF(3) //and the above term has to appear in at least 3 documents
  val fittedCV = cv.fit(dfWithWords)

  fittedCV.transform(dfWithWords).show(30, false)

  //we can print out vocabulary
  println(fittedCV.vocabulary.mkString(","))

}
