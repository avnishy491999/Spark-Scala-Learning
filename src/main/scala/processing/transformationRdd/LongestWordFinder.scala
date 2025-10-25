package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LongestWordFinder {

  def findLongestWord(sparkFlatMap:RDD[String],spark:SparkSession)={
    sparkFlatMap.reduce(longestWordFinder)
  }

  def longestWordFinder(left:String,right: String)={
    if(left > right){
      left
    }
    else{
      right
    }
  }
}
