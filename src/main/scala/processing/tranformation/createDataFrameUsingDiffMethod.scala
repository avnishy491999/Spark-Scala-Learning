package processing.tranformation

import org.apache.spark.sql.SparkSession

object createDataFrameUsingDiffMethod {

  def run(spark:SparkSession)={

    val sequenceDf = createDataFrameUsingSeq.usingSequence(spark)
    val caseClassDf = createDataFrameUsingCaseClass.usingCaseClass(spark)
    val rddDf: Unit = createDataFrameUsingRdd.usingRdd(spark)
    val schemDf: Unit = createDataFrameUsingSchema.usingSchema(spark)
    val ListDf: Unit = createDataFrameUsingList.usingList(spark)
  }

}
