package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import processing.model.Person

object createArrayOfCars {

  def createArray(spark: SparkSession): RDD[String] = {

    val carsArray = Array("audi", "BMW", "Bentley", "Suzuki", "BMW", "Honda")
    val carsRDD = spark.sparkContext.parallelize(carsArray)
    carsRDD
  }
}
