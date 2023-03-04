package com.unresolved.preparation
import org.apache.spark.sql.DataFrame
import com.unresolved.SparkUtils
import com.unresolved.reader.{AbstractReaderDf, AvroReader, CSVReader, JSONReader}
import com.unresolved.utils.MySleep.uemSleep
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, date_format, desc, explode, expr, locate, row_number, to_timestamp}

import java.time.{Instant, ZoneId}
import java.util.Locale
import java.time.format.DateTimeFormatter
object Pregunta02 extends SparkUtils {
  def main(args: Array[String]): Unit = {
    //val path_twitter: String = getClass.getResource("05.json").getPath
    val jsonReader = JSONReader()
    val df_twitter: DataFrame = jsonReader.readDf("C:\\Users\\LENNON\\Desktop\\Master en BIG DATA\\Sistemas Distribuidos\\eu-sistemas-distribuidos\\eu-twitter\\src\\main\\resources\\05.json")

    import spark.implicits._

    //println("Trending topic en función de cuatro idiomas del perfil de usuario: ")

    ////////////////////////////////
    println("Solución 1, ya que en el json no estructurado aparece la variable lang en 3 oportunidades")
    val df_drop = df_twitter.drop("delete")
    val drop_null_col = df_drop.na.drop(Seq("created_at"))

    import spark.implicits._

    println("e) Los trending topics según el idioma del perfil de usuario: ")
    //CON LOS DATOS PREVIOS Y FILAS CON NULL FILTRADAS OBTENEMOS LAS SIGUIENTES COLUMNAS CON SUS ALIAS
    val e = drop_null_col
      .select(col("user.id"), $"lang".as("lenguaje"), $"entities.hashtags".as("hashtags"))
    //HACEMOS UN COUNT PARA VER QUE LENGUAGES TIENEN MAYOR CANTIDAD DE USO Y AGRUPAMOS LOS 4 PRIMEROS PARA SU USO Y LOS IMPRIMIMOS
    val count_lenguaje = e.groupBy(col("lenguaje")).count().orderBy(col("count").desc)
      .select(collect_list(col("lenguaje"))).as[Array[String]].collect()(0).take(4)
    println(count_lenguaje.mkString(", "))

    // Agregar una columna con el lenguaje de cada usuario con los 4 primeros idiomas
    val usersWithLanguage = e.withColumn("user_language", col("lenguaje")).filter(col("lenguaje").isin(count_lenguaje: _*))

    // Crear una nueva fila por cada hashtag en la columna "hashtags"
    val hashtagsDF = usersWithLanguage.select(col("user_language"), explode(col("hashtags.text")).alias("hashtag"))


    // Contar el número de apariciones de cada hashtag por lenguaje
    val countedHashtags = hashtagsDF.groupBy(col("user_language"), col("hashtag")).count()

    // Ordenar los hashtags por lenguaje y por el número de apariciones
    val topHashtagsByLanguage = countedHashtags.orderBy(col("user_language"), col("count").desc)
      .withColumn("rank", row_number().over(Window.partitionBy("user_language").orderBy(col("count").desc)))
      .filter(col("rank") <= 1)

    // Mostrar el resultado
    topHashtagsByLanguage.show(200)

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

    //println("El usuario con mayor número de seguidores que participó ese día: ")
    val follower = df_twitter
      .select($"user.id",$"user.followers_count")
      .orderBy(desc("user.followers_count"))
      .show(1, false)



  }
}