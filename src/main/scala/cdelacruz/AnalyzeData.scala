package cdelacruz
import Functions._
import org.apache.spark.sql.functions._
import ReadData._

object AnalyzeData extends App {

  lazy val df_insta = my_spark.read.parquet("src/main/resources/single_file/instagram.parquet")

  df_insta.show(5)

  //Amount of records.
  println("n_reg: "+ df_insta.count())

  //Years of information
  df_insta.select(
    year(col("timestamp_post")).as("year_post")
  )
    .groupBy(col("year_post"))
    .agg(
      count(col("*")).as("n_posts")
    )
    .distinct()
    .orderBy(desc_nulls_last("year_post"))

  //Check increase of records through the time.
  df_insta.select(
    date_format(col("timestamp_post"),"yyyy").as("f_post_ts")
  )
    //.where(year(col("timestamp_post")) === 2015)
    .where(col("timestamp_post").isNotNull)
    .groupBy(col("f_post_ts"))
    .agg(
      count("*").as("n_reg")
    )
    .orderBy(desc_nulls_last("f_post_ts"))

  //Particionar en hora para conocer tambien las horas mas activas en post (por año).
  df_insta.select(
    year(col("timestamp_post")).as("year_post"),
    hour(col("timestamp_post")).as("hour_post")
  )
    //.where(year(col("timestamp_post")) === 2015)
    .where(
      col("timestamp_post").isNotNull
        and year(col("timestamp_post")) === 2015
    )
    .groupBy(col("year_post"),col("hour_post"))
    .agg(
      count("*").as("n_reg")
    )
    .orderBy(desc_nulls_last("n_reg"))

  //Particionar en hora para conocer tambien las horas mas activas en post (por año y mes).
  df_insta.select(
    year(col("timestamp_post")).as("year_post"),
    month(col("timestamp_post")).as("month_post"),
    hour(col("timestamp_post")).as("hour_post")
  )
    //.where(year(col("timestamp_post")) === 2015)
    .where(
      col("timestamp_post").isNotNull
      and year(col("timestamp_post")) === 2015
      and month(col("timestamp_post")) === 12
    )
    .groupBy(col("year_post"),col("month_post"), col("hour_post"))
    .agg(
      count("*").as("n_reg")
    )
    .orderBy(desc_nulls_last("n_reg"))

  //Palabra mas utilizada en los post.
  df_insta.select(
    "post_id",
    "description_post"
  )
    .where(year(col("timestamp_post")) === 2015)
    .withColumn("split_words", explode(split(col("description_post")," ")))
    .where(
      year(col("timestamp_post")) === 2015
      and col("split_words").like("#%")
    )
    .groupBy(col("split_words"))
    .agg(
      count("*").as("count")
    )
    .orderBy(desc_nulls_last("count"))

  //Hashtag mas utilizado en los post.
  df_insta.select(
    "post_id",
    "description_post"
  )
    .withColumn("split_words", explode(split(col("description_post"), " ")))
    .groupBy(col("split_words"))
    .agg(
      count("*").as("count")
    )
    .orderBy(desc_nulls_last("count"))

  //Lugar mas visitado.
  df_locations.groupBy(col("cd"), col("name"))
    .agg(
      count("*").as("n_reg")
    )
    .where(col("cd").isNotNull and col("name").isNotNull)
    .orderBy(desc_nulls_last("n_reg"))

  //Lugar mas visitado (por paises).
  df_locations.groupBy(
    when(col("cd") === "US", "United States")
      .otherwise(col("cd")).as("cd")
  )
    //.groupBy(col("cd"))
    .agg(
      count("*").as("n_reg")
    )
    .where(col("cd").isNotNull)
    .orderBy(desc_nulls_last("n_reg")).show()
  //Cuales son los post con mas me gusta o comentarios
  //Tipo de posteo con mayor impacto por año.
  df_posts.select(
    year(col("cts")).as("year_post"),
    when(col("post_type") === 1, "photo")
      .when(col("post_type") === 2, "video")
      .when(col("post_type") === 3, "multy")
      .as("post_type"),
    col("numbr_likes"),
    col("number_comments")
  )
    .groupBy(col("year_post"), col("post_type"))
    .agg(
      sum("numbr_likes").as("number_likes"),
      sum("number_comments").as("number_comments")
    )
    .where(col("year_post").isNotNull)
    .orderBy(
      desc("year_post"),
      desc("number_likes"),
      desc("number_comments")
    )
    .show()
  //ver las caracteristicas de los perfiles con mas seguidores.
  //Tipo de posteo que más se realiza por año.
  df_posts.select(
    year(col("cts")).as("year_post"),
    when(col("post_type")===1,"photo")
      .when(col("post_type")===2,"video")
      .when(col("post_type")===3,"multy")
      .as("post_type")
    )
    .groupBy(col("year_post"),col("post_type"))
    .agg(
      count("*").as("n_reg")
    )
    .where(col("year_post").isNotNull)
    .orderBy(desc("year_post"),desc("n_reg"))


  //Cuales son las fechas de mayor posteo por año.
  df_posts.select(
    col("cts"),
    dayofweek(col("cts")).as("day_of_week")
  )

  //Amount of users per contry.
  df_insta.select(
    col("profile_id"),
    col("country_code")
  )
    .where(col("country_code").isNotNull)
    .dropDuplicates("profile_id")
    .groupBy(
      col("country_code")
    )
    .agg(
      count("*").as("n_reg")
    )
    .orderBy(desc_nulls_last("n_reg"))

}
