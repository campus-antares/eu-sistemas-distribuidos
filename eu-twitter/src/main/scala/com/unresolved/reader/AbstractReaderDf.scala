package com.unresolved.reader

import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class AbstractReaderDf {

  def readDf(path: String)(implicit spark: SparkSession): DataFrame

}
