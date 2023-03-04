package com.unresolved.preparation
import org.apache.spark.sql.DataFrame
import com.unresolved.SparkUtils
import com.unresolved.reader.{AbstractReaderDf, AvroReader, CSVReader, JSONReader}
import com.unresolved.utils.MySleep.uemSleep
import org.apache.spark.sql.functions.{col, count, date_format, desc, explode, expr, locate, to_timestamp}

import java.time.{Instant, ZoneId}
import java.util.Locale
import java.time.format.DateTimeFormatter
object Pregunta01 extends SparkUtils {
  def main(args: Array[String]): Unit = {
  //val path_twitter: String = getClass.getResource("C:\\Users\\LENNON\\Desktop\\Master en BIG DATA\\Sistemas Distribuidos\\eu-sistemas-distribuidos\\eu-twitter\\src\\main\\resources\\05.json").getPath
  val jsonReader = JSONReader()
  val df_twitter: DataFrame = jsonReader.readDf("C:\\Users\\LENNON\\Desktop\\Master en BIG DATA\\Sistemas Distribuidos\\eu-sistemas-distribuidos\\eu-twitter\\src\\main\\resources\\05.json")
  val df_drop = df_twitter.drop("delete")

  val drop_null_col = df_drop.na.drop(Seq("created_at"))

  import spark.implicits._

    println("Cantidad de datos procesados en función de cada intervalo temporal (hora y día): ")
  val b = drop_null_col
    .select($"id", $"created_at")
    .withColumn("day",date_format(to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy"),"dd"))
    .withColumn("hour", date_format(to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy"), "hh"))

    //.show(10, false)

  // agrupamos los datos por hora y día y contamos la cantidad de filas
  val result = b.groupBy("hour", "day").agg(count("*").as("data counted"))
    .show(10, false)

    println("Esquema JSON del contenido de la fuente de datos: ")
    drop_null_col.printSchema()

    println("Los 10 hashtags con mayor número de apariciones (trending topic): ")

    val extended_tweet = df_twitter
      .select(explode(col("retweeted_status.extended_tweet.entities.hashtags")).alias("hashtag"))
    // seleccionamos la columna "text" de la columna "hashtag"
    val extended_tweet_text:DataFrame = extended_tweet.select(col("hashtag.text").alias("hashtag-extended"))
      //.show(10, false)

    val entities = df_twitter
      .select(explode(col("retweeted_status.entities.hashtags")).alias("hashtag"))
    // seleccionamos la columna "text" de la columna "hashtag"
    val entities_text:DataFrame = entities.select(col("hashtag.text").alias("hashtag-entities"))
      //.show(10, false)

    val only_entities = df_twitter
      .select(explode(col("entities.hashtags")).alias("hashtag"))
    // seleccionamos la columna "text" de la columna "hashtag"
    val only_entities_text:DataFrame = only_entities.select(col("hashtag.text").alias("hashtag-only-entities"))
      //.show(10, false)

    val unionHashtag = extended_tweet_text.union(entities_text).union(only_entities_text)

    val df_final = unionHashtag
      .groupBy($"hashtag-extended")
      .agg(count("*").as("hashtag counted"))
      .orderBy(desc("hashtag counted"))
    df_final.show(10, false)

    println("Trending topic en función de cuatro idiomas del perfil de usuario: ")
    val extended_lang = df_twitter
      .select(explode(col("retweeted_status.extended_tweet.entities.hashtags")).alias("hashtag"))
    // seleccionamos la columna "text" de la columna "hashtag"
    //val extended_tweet_text: DataFrame = extended_tweet.select(col("hashtag.text").alias("hashtag-extended"))
    .show(10, false)
  }
}