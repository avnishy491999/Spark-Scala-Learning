package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BookRddRun {

  def Run(bookRDD:RDD[String], spark:SparkSession): RDD[String] ={

    //map transformation
    val bookMapRdd = BookRddTransform.mapTransFormation(bookRDD,spark)

    //flatMap transformation
    val bookflatMapRdd = BookRddTransform.flatMapTransFormation(bookRDD,spark)

   // sort the words by letters
    val sortBooksRdd = BookRddTransform.sortByTransformation(bookflatMapRdd,spark)

    sortBooksRdd.cache()

    sortBooksRdd.take(5)
    sortBooksRdd.top(5)

    //countByValueTransformation
    val countBooksRdd = BookRddTransform.countByValueTransformation(bookflatMapRdd,spark)
    println("countByvalue transformation")
    countBooksRdd.take(10).foreach(println)

    //saving as text file
   // val saveRddAsTextFile= BookRddTransform.saveAsTextFileTransformation(sortBooksRdd,spark)


    bookflatMapRdd
  }
}
