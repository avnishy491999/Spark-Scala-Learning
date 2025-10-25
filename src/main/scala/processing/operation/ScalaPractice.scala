package processing.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import processing.transformationRdd.ScalaTransformation

object ScalaPractice {

  def run(spark:SparkSession)={
    //First will see how flatMap works in scala
    //Example 1=>
    val scalaFlatMap: Unit = ScalaTransformation.Transform(spark)
  }
}
