package processing.main

import org.apache.spark.sql.SparkSession
import processing.operation.{RddDay1Transformation, RddDay2Transformation, RddDay3Transformation, ScalaPractice}
import processing.tranformation.{createDataFrameUsingCaseClass, createDataFrameUsingDiffMethod, createDataFrameUsingList, createDataFrameUsingRdd, createDataFrameUsingSchema, createDataFrameUsingSeq}

object demo{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("demo").master("local[*]").getOrCreate()
    println("spark session is created")

    //different ways to create a dataframe
   // val dataFrameUsingDiffMethods = createDataFrameUsingDiffMethod.run(spark)

    val rddDay1 = RddDay1Transformation.run(spark)
    val scalaPractice = ScalaPractice.run(spark)
    val rddDay2 = RddDay2Transformation.run(spark)
  //val rddDay3 = RddDay3Transformation.run(spark)

//    println("Press ENTER to exit and close Spark UI...")
//    scala.io.StdIn.readLine()
//    spark.stop()


  }
}
