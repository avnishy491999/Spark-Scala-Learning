package processing.operation

import org.apache.spark.sql.SparkSession
import processing.transformationRdd.{CarsRddRun, createArrayOfCars}

object RddDay1Transformation {

  def run(spark:SparkSession) ={

    //1. create carsArray
    val carsRdd = createArrayOfCars.createArray(spark)

    val carsRddTransform: Unit = CarsRddRun.Run(carsRdd,spark)
  }
}
