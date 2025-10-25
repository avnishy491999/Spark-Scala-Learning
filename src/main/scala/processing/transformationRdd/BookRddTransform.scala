package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BookRddTransform {

 def mapTransFormation(bookRDD:RDD[String], spark:SparkSession): RDD[Array[String]] ={
   val bookMapRdd = bookRDD.map(word=>word.split(","))
   bookMapRdd
 }

  def flatMapTransFormation(bookRDD:RDD[String], spark:SparkSession): RDD[String] ={
    val bookFlatMapRdd = bookRDD.flatMap(word=>word.split(" "))
    bookFlatMapRdd
  }

  def sortByTransformation(bookRDD:RDD[String],spark:SparkSession): RDD[String] ={
    bookRDD.sortBy(x=>x.length)
  }

  def countByValueTransformation(bookflatMapRdd:RDD[String],spark:SparkSession): collection.Map[String, Long] ={
    bookflatMapRdd.countByValue()
  }

  def saveAsTextFileTransformation(sortBooksRdd:RDD[String],spark:SparkSession): Unit ={
    //we used coalese 1.
    //because we want to store the data in one file. otherwise it would have stored the data in 2
    //file as we have given the no of partition =2 earlier
    sortBooksRdd
      .coalesce(1)
      .saveAsTextFile("/Users/avnishyadav/Downloads/sorted_book_directory2")
  }

  def coalesceTransformation(sortBooksRdd:RDD[String],spark:SparkSession): RDD[String] ={
    sortBooksRdd.coalesce(1)
  }
}
