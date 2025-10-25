package processing.transformationRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ShortestWordFinder {

  def findShortestWord(bookRDD:RDD[String],spark:SparkSession)={
    val res = bookRDD.reduce(smallestWord)
    println("smallest word")
    println(res)
  }

  def smallestWord(leftWord:String,rightWord:String)={
    if(leftWord.length> rightWord.length) {
       rightWord
    } else {
       leftWord
    }
  }
}
