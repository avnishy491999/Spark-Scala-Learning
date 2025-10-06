package processing.tranformation

import org.apache.spark.sql.SparkSession
import processing.model.Person

object createDataFrameUsingList {

  def usingList(spark:SparkSession)={
    val apList = List(
      (1,"avnish","avnish@gmail.com"),
      (1,"indu","indu@gmail.com"),
      (1,"yogi","yogi@gmail.com")
    )

    val apSeq = Seq("id","name","Email")

    val apRdd = spark.sparkContext.parallelize(apList)
    val apDf = spark.createDataFrame(apRdd)
    val apDf2 = apDf.toDF(apSeq:_*)
    println("apDf2")
    apDf2.show(10,false)
  }
}
