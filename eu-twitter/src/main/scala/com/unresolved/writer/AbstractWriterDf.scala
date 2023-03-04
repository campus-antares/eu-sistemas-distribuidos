package com.unresolved.writer

import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class AbstractWriterDf {

  def writeDf(path: String)(implicit spark: SparkSession): DataFrame

}
