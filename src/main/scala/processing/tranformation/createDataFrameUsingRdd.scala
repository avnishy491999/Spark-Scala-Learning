package processing.tranformation

import org.apache.spark.sql.SparkSession
import processing.model.Person

object createDataFrameUsingRdd {

  def usingRdd(spark:SparkSession)={
import spark.implicits._

val rdd1 = Seq(
  ("Alice",25),
  ("bob",23)
)

    val rdd = spark.sparkContext.parallelize(rdd1).toDF()
    val rddDf = rdd.toDF("name","age")
    println("rddDf")
    rddDf.show(10,false)
  }
}
