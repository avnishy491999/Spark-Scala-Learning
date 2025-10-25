package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CarsRddRun {

  def Run(carsRdd:RDD[String], spark:SparkSession)={

    //1. find the distinct element
    val distinctCarsRdd = carsRddTransForm.distinctTransform(carsRdd,spark)

    //2. filter the car which starts from B
    val filteredRdd = carsRddTransForm.filterTransform(distinctCarsRdd,spark)

    //3. lets say we have numbersRdd from 1 to 10 find out the even numbers
    val evenNumRdd =carsRddTransForm.findEvenNumbers(spark)

    //4. Map
    //Example -1 =>we have distinctCarsRdd. filter out the cars which startswith B
    val mappedCarsWithBRdd = carsRddMapTransformation.filterUsingMap(distinctCarsRdd,spark)

    //Example 2 =>find out the carname and length of the car
    val mappedCarsWithLengthRdd: Unit = carsRddMapTransformation.filterToFindLength(distinctCarsRdd,spark)
  }
}
