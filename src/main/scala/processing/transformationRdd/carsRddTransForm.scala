package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object carsRddTransForm {
  def distinctTransform(carsRdd:RDD[String],spark:SparkSession): RDD[String] = {
    val distinctCarsRdd = carsRdd.distinct
    println("distinctCarsRdd")
    println(distinctCarsRdd.collect().mkString(","))
    distinctCarsRdd
  }

  def filterTransform(distinctCarsRdd:RDD[String],spark:SparkSession): RDD[String] ={
    val filteredRdd = distinctCarsRdd.filter(x=>x.startsWith("B"))
    println("filteredRdd")
    println(filteredRdd.collect().mkString(","))
    filteredRdd
  }

  def findEven(value:Int): Boolean ={
    value % 2==0
  }

  def findEvenNumbers(spark:SparkSession): Unit ={
    val numsRdd = spark.sparkContext.parallelize(1 to 10,2)
    val evenNums = numsRdd.filter(findEven)
    println("evenNums")
    println(evenNums.collect().mkString(","))
    evenNums
  }
}
