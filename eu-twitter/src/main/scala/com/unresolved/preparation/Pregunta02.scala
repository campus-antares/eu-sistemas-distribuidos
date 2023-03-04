package com.unresolved.preparation
import org.apache.spark.sql.DataFrame
import com.unresolved.SparkUtils
import com.unresolved.reader.{AbstractReaderDf, AvroReader, CSVReader, JSONReader}
import com.unresolved.utils.MySleep.uemSleep
import org.apache.spark.sql.functions.{col, count, date_format, desc, explode, expr, locate, to_timestamp}
import java.time.{Instant, ZoneId}
import java.util.Locale
import java.time.format.DateTimeFormatter
object Pregunta02 extends SparkUtils {
  def main(args: Array[String]): Unit = {
    //val path_twitter: String = getClass.getResource("C:\\Users\\LENNON\\Desktop\\Master en BIG DATA\\Sistemas Distribuidos\\eu-sistemas-distribuidos\\eu-twitter\\src\\main\\resources\\05.json").getPath
    val jsonReader = JSONReader()
    val df_twitter: DataFrame = jsonReader.readDf("C:\\Users\\LENNON\\Desktop\\Master en BIG DATA\\Sistemas Distribuidos\\eu-sistemas-distribuidos\\eu-twitter\\src\\main\\resources\\05.json")
    import spark.implicits._

    println("Trending topic en función de cuatro idiomas del perfil de usuario: ")
    val entities_lang = df_twitter
      .select($"lang".as("user_lang"), explode(col("entities.hashtags.text")).alias("hashtag"))
      /*.groupBy("user_lang","hashtag")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))*/

      val extended_tweet_lang:DataFrame = entities_lang
      //.show(10, false)

    val extended_lang = df_twitter
      .select($"lang".as("user_lang"),explode(col("retweeted_status.extended_tweet.entities.hashtags.text")).alias("hashtag"))
      /*.groupBy("user_lang", "hashtag")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))*/

    // seleccionamos la columna "text" de la columna "hashtag"
    val extended_tweet_text: DataFrame = extended_lang
    //.show(10, false)
    val unionLang = extended_tweet_text.union(extended_tweet_lang)

    val topicLang = unionLang
      .groupBy("user_lang")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
    .show(4,false)

    println("El usuario con mayor número de seguidores que participó ese día: ")
    val follower = df_twitter
      .select($"user.id",$"user.followers_count")
      .orderBy(desc("user.followers_count"))
      .show(1, false)



  }
}