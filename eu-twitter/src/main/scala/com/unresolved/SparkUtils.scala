package com.unresolved

import org.apache.spark.sql.SparkSession

trait SparkUtils {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DB Interview")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


}
