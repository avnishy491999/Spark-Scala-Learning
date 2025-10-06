package processing.tranformation

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import processing.model.Person

object createDataFrameUsingSchema {

  def usingSchema(spark:SparkSession)={
    val schema = StructType(
      Array(
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )

    val data = Seq(
      Row("Alice",34),
      Row("Bob",25)
    )

    val data1 = Seq(
      ("Alice",34),
      ("Bob",25)
    )
    val rdd = spark.sparkContext.parallelize(data)
    val schemaDf = spark.createDataFrame(rdd,schema)

  }
}
