package com.unresolved.preparation
import org.apache.spark.sql.DataFrame
import com.unresolved.SparkUtils
import com.unresolved.reader.JSONReader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, date_format, desc, explode, row_number, to_timestamp}

object Pregunta01 extends SparkUtils {
  def main(args: Array[String]): Unit = {
  val jsonReader = JSONReader()
  val df_twitter: DataFrame = jsonReader.readDf("/Users/rudy/Documents/05.json")
  val df_drop = df_twitter.drop("delete")

  val drop_null_col = df_drop.na.drop(Seq("created_at"))

  import spark.implicits._


    /**
     * Cantidad de datos procesados en función de cada intervalo temporal (hora y día):
     *  1. Se agreha dos nuevas columnas la tener la hora y dia,
     *  para ello se usa el to_timestamp y luego se castea con date_format
      */

   println("PREGUNTA B: Cantidad de datos procesados en función de cada intervalo temporal (hora y día):")
  val b = drop_null_col
    .select($"id", $"created_at")
    .withColumn("day",date_format(to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy"),"dd"))
    .withColumn("hour", date_format(to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy"), "hh"))

    // agrupamos los datos por hora y día y contamos la cantidad de filas
     val result = b.groupBy("hour", "day").agg(count("*").as("data counted"))
    .show(10, false)

    println("PREGUNTA C: Esquema JSON del contenido de la fuente de datos")
    drop_null_col.printSchema()

    println("PREGUNTA D: Los 10 hashtags con mayor número de apariciones (trending topic)")
    val extended_tweet = df_twitter
      .select(explode(col("retweeted_status.extended_tweet.entities.hashtags")).alias("hashtag"))

    val extended_tweet_text:DataFrame = extended_tweet.select(col("hashtag.text").alias("hashtag-extended"))

    val entities = df_twitter
      .select(explode(col("retweeted_status.entities.hashtags")).alias("hashtag"))
    val entities_text:DataFrame = entities.select(col("hashtag.text").alias("hashtag-entities"))

    val only_entities = df_twitter
      .select(explode(col("entities.hashtags")).alias("hashtag"))
    val only_entities_text:DataFrame = only_entities.select(col("hashtag.text").alias("hashtag-only-entities"))

    //Unificamos los dataframe anteriores
    val unionHashtag = extended_tweet_text.union(entities_text).union(only_entities_text)

    val df_final = unionHashtag
      .groupBy($"hashtag-extended")
      .agg(count("*").as("hashtag counted"))
      .orderBy(desc("hashtag counted"))
    df_final.show(10, false)

    println("PREGUNTA E: Trending topic en función de cuatro idiomas del perfil de usuario.")


  }
}