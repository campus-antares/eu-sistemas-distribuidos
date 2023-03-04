package com.unresolved.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class ParquetReader(options: Map[String, String]) extends AbstractReaderDf {

  override def readDf(path: String)(implicit spark: SparkSession): DataFrame = ???

}
