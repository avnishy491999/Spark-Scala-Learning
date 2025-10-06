package processing.main

import org.apache.spark.sql.SparkSession
import processing.tranformation.{createDataFrameUsingCaseClass, createDataFrameUsingList, createDataFrameUsingRdd, createDataFrameUsingSchema, createDataFrameUsingSeq}

object demo{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("demo").master("local[*]").getOrCreate()
    println("spark session is created")

    //different ways to create a dataframe

//    val sequenceDf = createDataFrameUsingSeq.usingSequence(spark)
//
//    val caseClassDf = createDataFrameUsingCaseClass.usingCaseClass(spark)

  // val rddDf = createDataFrameUsingRdd.usingRdd(spark)
//
//    val schemDf = createDataFrameUsingSchema.

   // val ListDf = createDataFrameUsingList.usingList(spark)

    val carsArray = Array("BMW","Audi","bentley","Mercedes","suzuki","TATA")
    val carsRDD = spark.sparkContext.parallelize(carsArray,2)

    val carsWithBrdd = carsRDD.map(x=>(x,x.startsWith("B")))

    val carsWithLengthRdd = carsRDD.map(carname=>(carname,carname.length))
    println("carsRDD")
    println(carsWithLengthRdd.collect().mkString(","))




  }
}
