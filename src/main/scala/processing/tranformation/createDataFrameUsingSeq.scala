package processing.tranformation

import org.apache.spark.sql.SparkSession

object createDataFrameUsingSeq {

  def usingSequence(spark:SparkSession)={

    val Seq1 = Seq(
      ("avnish",25),
      ("Bob","23")
    )

    val sequenceDf = spark.createDataFrame(Seq1).toDF("name","age")
    println("sequenceDf")
    sequenceDf.show(2,false)
    sequenceDf
  }

}
