package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object createArrayOfBook {

  def createArray(spark:SparkSession): RDD[String] ={

    val book = Array(
      "Some students save money by buying used copies of textbooks, which tend to be less expensive.",
      "and are available from many college bookstores in the US who buy them back from students at the end of a term. Books that are not being re-used at the school are often purchased by an off-campus wholesaler for 0–30% of the new cost.",
      "for distribution to other bookstores. Some textbook companies have countered this by encouraging teachers to assign homework that must be done on the publisher's website.",
      "Students with a new textbook can use the pass code in the book to register on the site otherwise they must pay the publisher to access the website and complete assigned homework."
    )
    val bookRdd = spark.sparkContext.parallelize(book,2)
    bookRdd
  }
}
