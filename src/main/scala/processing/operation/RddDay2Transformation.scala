package processing.operation

import org.apache.spark.sql.SparkSession
import processing.transformationRdd.{BookRddRun, LongestWordFinder, ShortestWordFinder, createArrayOfBook}

object RddDay2Transformation {

def run(spark:SparkSession)={
  //2 .now create a bookArray
  val bookRDD = createArrayOfBook.createArray(spark)

  //now lets do with RDD in spark
  val sparkFlatMap = BookRddRun.Run(bookRDD,spark)

  //Q1. we have book Rdd, now write a program to find the shortest word form the book Rdd
  val shortestWord = ShortestWordFinder.findShortestWord(sparkFlatMap,spark)

  val longestWord = LongestWordFinder.findLongestWord(sparkFlatMap,spark)
  sparkFlatMap
}

}
