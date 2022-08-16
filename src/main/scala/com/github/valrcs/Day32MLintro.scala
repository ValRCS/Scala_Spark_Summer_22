package com.github.valrcs

import org.apache.spark.mllib.linalg.Vectors

object Day32MLintro extends App {

  //Low-level data types
  //In addition to the structural types for building pipelines, there are also several lower-level data
  //types you may need to work with in MLlib (Vector being the most common). Whenever we pass
  //a set of features into a machine learning model, we must do it as a vector that consists of
  //Doubles. This vector can be either sparse (where most of the elements are zero) or dense (where
  //there are many unique values). Vectors are created in different ways. To create a dense vector,
  //we can specify an array of all the values. To create a sparse vector, we can specify the total size
  //and the indices and values of the non-zero elements. Sparse is the best format, as you might have
  //guessed, when the majority of values are zero as this is a more compressed representation. Here
  //is an example of how to manually create a Vector:

  //not to be confuses with Scala Vector which is different (but actually has many similarities)
  val denseVec = Vectors.dense(1.0, 2.0, 3.0)
  println(denseVec)
  val arrDoubles = denseVec.toArray //we can convert to Array when we need to
  println(arrDoubles.mkString(","))

  //sparse Vector
  val size = 15
  val idx = Array(1,6,9) // locations of non-zero elements in vector
  val values = Array(2.0,3.0, 50.0)
  //useful for storing data when there are a lot of missing values
  val sparseVec = Vectors.sparse(size, idx, values)
  println(sparseVec)

  val arrDoublesAgain = sparseVec.toArray
  println(arrDoublesAgain.mkString(","))


}
