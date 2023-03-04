package com.resolved.reader

import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class ReaderDf {

  def readDf(path: String)(implicit spark: SparkSession): DataFrame

}
