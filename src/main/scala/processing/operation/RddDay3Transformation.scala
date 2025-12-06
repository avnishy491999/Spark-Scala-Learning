package processing.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddDay3Transformation {

def run(flatMapRdd:RDD[String],spark:SparkSession)={

  //map to create key value pair rdd

  val keyValueRdd = flatMapRdd.map(word=>(word,word.toLowerCase))
  println("keyValueRdd")
  println(keyValueRdd.collect().mkString(","))
  keyValueRdd.collect().foreach(println)

  val wordAndLengthRdd = flatMapRdd.map(x=>(x,x.length))
  println("wordAndLengthRdd")
 println(wordAndLengthRdd.collect().mkString(","))
  wordAndLengthRdd.foreach(println)

}



}
