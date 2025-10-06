package processing.tranformation

import org.apache.spark.sql.SparkSession
import processing.model.Person

object createDataFrameUsingCaseClass {

  def usingCaseClass(spark:SparkSession)={

    val caseClassSeq = Seq(
      Person("Avnish",25,150000),
      Person("BOB",23,60000),
      Person("Monu",26,120000)
    )

    val caseClassdf = spark.createDataFrame(caseClassSeq)
    println("caseClassdf")
    caseClassdf.show(10,false)
    caseClassdf
  }
}
