package cdelacruz

import Functions._
import org.apache.spark.sql.functions._
import ReadData._
import org.apache.spark.sql.types.DecimalType
import scala.concurrent.duration._

object AnalyzeData extends App {
  //Time calculation begins
  val t_ini = System.nanoTime()


  lazy val df_insta = my_spark.read.parquet("src/main/resources/single_file/instagram.parquet")

  df_insta
    //.show(10)

  //Amount of records.
  //println("n_reg: "+ df_insta.count())

  //Amount of records per year.
  df_insta.select(
    year(col("timestamp_post")).as("year_post")
  )
    .where(col("year_post").isNotNull)
    .groupBy(col("year_post"))
    .agg(
      count(col("*")).as("n_posts")
    )
    .orderBy(desc_nulls_last("year_post"))
    //.show()

  //Check amount of records through the time.
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

  //Divide by time to also know the most active hours in the publications (by year).
  df_insta.select(
    year(col("timestamp_post")).as("year_post"),
    hour(col("timestamp_post")).as("hour_post")
  )
    //.where(year(col("timestamp_post")) === 2015)
    .where(
      col("timestamp_post").isNotNull
        //and year(col("timestamp_post")) === 2015
    )
    .groupBy(col("year_post"),col("hour_post"))
    .agg(
      count("*").as("n_reg")
    )
    .orderBy(desc_nulls_last("n_reg"))
    //.show()

  //Divide by time to also know the most active hours in the publications (by year and month).
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

  //Hashtag most used in the posts.
  df_insta.select(
    "post_id",
    "description_post"
  )
    .withColumn(
      "hashtag",
      explode(filter(split(col("description_post")," "), x => x.startsWith("#")))
    )
    /*.withColumn("hashtag", explode(split(col("description_post")," ")))
    .where(
      col("hashtag").like("#%")
      //and year(col("timestamp_post")) === 2015
    )*/
    .groupBy(col("hashtag"))
    .agg(
      count("*").as("count")
    )
    .orderBy(desc_nulls_last("count"))
    //.show(10)

  //Most used word in the posts.
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

  //Most visited place.
  df_locations.groupBy(col("cd"), col("name"))
    .agg(
      count("*").as("n_reg")
    )
    .where(col("cd").isNotNull and col("name").isNotNull)
    .orderBy(desc_nulls_last("n_reg"))

  //Most visited place (by country).
  df_locations.groupBy(
    when(col("cd") === "US", "United States")
      .otherwise(col("cd")).as("country"),
    col("city")
  )
    //.groupBy(col("cd"))
    .agg(
      count("*").as("n_reg")
    )
    .where(
      col("country").isNotNull
      and col("city").isNotNull
    )
    .orderBy(desc_nulls_last("n_reg"))
    //.show()

  //Type of posts most made per year.
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
    //.show()

  //Type of post with the greatest impact per year.
  val df_impact_posts = df_posts.select(
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
    //.show()

  //writeCSV(df_impact_posts, ",", "src/main/resources/single_file", "impact_posts")

  //Day with the most posts per year.
  val df_date_x_post = df_posts.select(
    year(col("cts")).as("year_post"),
    dayofweek(col("cts")).as("day_of_week"),
    date_format(col("cts"), "EEEE").as("name_day_of_week")
  )
    .groupBy(
      col("year_post"),
      //col("day_of_week"),
      col("name_day_of_week")
    )
    .agg(
      count("*").as("n_reg")
    )
    .where(col("year_post").isNotNull)
    .orderBy(
      desc("year_post"),
      //col("day_of_week")
      desc("n_reg")
    )
    //.show()

  //writeCSV(df_date_x_post, ",", "src/main/resources/single_file", "date_x_post")

  //Amount characters per post.
  df_posts.select(
    year(col("cts")).as("year_post"),
    col("description")
  )
    .withColumn("characters", split(col("description"), ""))
    .withColumn("n_characters", size(col("characters")))
    .groupBy(col("year_post"))
    .agg(
      avg(col("n_characters")).cast(DecimalType(10,2)).as("avg_n_characters")
    )
    .where(col("year_post").isNotNull)
    .orderBy(desc("year_post"))
    //.show()

  //Relation between profiles en posts.
  val df_profile_post_impact_1 = df_insta.select(
    col("timestamp_post"),
    col("profile_id"),
    col("following"),
    col("followers"),
    col("is_business_account"),
    col("description_post"),
    col("post_type"),
    col("number_likes"),
    col("number_comments")
  )
    .withColumn(
      "hashtags",
      explode(split(col("description_post"), " "))
    )
    .withColumn("n_characters", size(split(col("description_post"), "")))
    .where(
      col("hashtags").like("#%")
      and col("profile_id").isNotNull
      and col("number_likes").isNotNull
      and col("number_comments").isNotNull
      and col("followers").isNotNull
      and col("n_characters").isNotNull
    )
    .groupBy(
      "timestamp_post",
      "profile_id",
      "following",
      "followers",
      "is_business_account",
      "description_post",
      "post_type",
      "number_likes",
      "number_comments",
      "n_characters"
    )
    .agg(
      count("*").as("n_hashtags")
    )

  //df_profile_post_impact_1.show(25)

  val df_profile_post_impact_2 = df_profile_post_impact_1.select(
    year(col("timestamp_post")).as("year"),
    col("profile_id"),
    col("following"),
    when(col("followers").between(0, 1000), "0_to_1k_followers")
      .when(col("followers").between(1001, 10000), "1k_to_10k_followers")
      .when(col("followers").between(10001, 100000), "10k_to_100k_followers")
      .when(col("followers").between(100001, 1000000), "100k_to_1m_followers")
      .when(col("followers")>1000000, "more_than_1m_followers")
    .as("followers"),
    col("is_business_account"),
    when(col("post_type") === 1, "photo")
      .when(col("post_type") === 2, "video")
      .when(col("post_type") === 3, "multy")
      .as("post_type"),
    col("number_likes"),
    col("number_comments"),
    when(col("n_characters").between(0, 50), "0_to_50_characters")
      .when(col("n_characters").between(51, 100), "50_to_100_characters")
      .when(col("n_characters").between(101, 150), "100_to_150_characters")
      .when(col("n_characters").between(151, 200), "150_to_200_characters")
      .when(col("n_characters").between(201, 250), "200_to_250_characters")
      .when(col("n_characters").between(251, 300), "250_to_300_characters")
      .when(col("n_characters") > 300, "more_than_300_characters")
      .as("number_characters"),
    when(col("n_hashtags").between(0, 10), "0_to_10_hashtags")
      .when(col("n_hashtags").between(11, 20), "10_to_20_hashtags")
      .when(col("n_hashtags").between(21, 30), "20_to_30_hashtags")
      .when(col("n_hashtags").between(31, 40), "30_to_40_hashtags")
      .when(col("n_hashtags").between(41, 50), "40_to_50_hashtags")
      .when(col("n_hashtags")>50, "more_than_50_hashtags")
      .as("number_hashtags")
  )

    val df_profile_post_impact = df_profile_post_impact_2.groupBy(
    col("year"),
    col("followers"),
    col("is_business_account"),
    col("post_type"),
      col("number_characters"),
    col("number_hashtags")
  )
      .agg(
        sum("number_likes").as("total_likes"),
        sum("number_comments").as("total_comments")
      )
      .orderBy(
        desc("year"),
        desc("total_likes"),
        desc("total_comments")
      )

  //df_profile_post_impact.show()
  //writeCSV(df_profile_post_impact, ",", "src/main/resources/single_file", "profile_post_impact")

  //Time calculation ends
  val t_fin = System.nanoTime()
  val duration = Duration(t_fin - t_ini, NANOSECONDS)
  println("Elapsed time is: " + duration.toSeconds + " seconds")
}