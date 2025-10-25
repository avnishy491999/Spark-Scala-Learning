package processing.transformationRdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ScalaTransformation {

  def Transform(spark:SparkSession)={
    val array1D = Array("1,2,3","4,5,6","7,8,9")
    val array2D = array1D.map(x=>x.split(","))
    println("array2D")
    println(array2D.map(_.mkString(",")).mkString(" | "))

    val arrayFlatD = array1D.flatMap(x=>x.split(","))
    println("flatmapRdd")
    println(arrayFlatD.mkString(","))

    val rdd1To10 = spark.sparkContext.parallelize(1 to 9)
    val reducedRdd = rdd1To10.reduce((x,y)=>x+y)
    println("reducedRdd")
    println(reducedRdd)
  }
}
