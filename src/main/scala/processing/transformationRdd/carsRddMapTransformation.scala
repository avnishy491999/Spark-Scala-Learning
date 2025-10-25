package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object carsRddMapTransformation {

  def filterUsingMap(distinctCarsRdd:RDD[String],spark:SparkSession): RDD[(String, Boolean)] ={
    val carsWithBRdd = distinctCarsRdd.map(x=>(x,x.startsWith("B")))
    val finalRdd = carsWithBRdd.filter(x=> x._2)
    println("finalRdd")
    println(finalRdd.collect().mkString(","))
    finalRdd
  }

  def filterToFindLength(distinctCarsRdd:RDD[String],spark:SparkSession)={
    val mapCarsWithLengthRdd = distinctCarsRdd.map(x=>(x,x.length))
    println("mapCarsWithLengthRdd")
    println(mapCarsWithLengthRdd.collect().mkString(","))
  }


}
