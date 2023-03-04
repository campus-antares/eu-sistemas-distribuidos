package com.unresolved.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AvroReader(options: Map[String, String]  = Map()) extends AbstractReaderDf {

  override def readDf(path: String)(implicit spark: SparkSession): DataFrame = spark
    .read
    .format("avro")
    .load(path)

}
