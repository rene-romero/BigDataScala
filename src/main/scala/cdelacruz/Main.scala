package cdelacruz

import Functions._
import org.apache.spark.sql.functions._
import ReadData._
import org.apache.spark.sql.types.DecimalType
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    //Time calculation begins
    val t_ini = System.nanoTime()


    /**
      * instagram.parquet as args(0)
      * profile_post_impact.csv as args(1)
      **/

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    val df = my_spark.read.parquet(args(0))

    val df_final_1 = df.select(
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

    val df_final_2 = df_final_1.select(
      year(col("timestamp_post")).as("year"),
      col("profile_id"),
      col("following"),
      when(col("followers").between(0, 1000), "0_to_1k_followers")
        .when(col("followers").between(1001, 10000), "1k_to_10k_followers")
        .when(col("followers").between(10001, 100000), "10k_to_100k_followers")
        .when(col("followers").between(100001, 1000000), "100k_to_1m_followers")
        .when(col("followers") > 1000000, "more_than_1m_followers")
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
        .when(col("n_hashtags") > 50, "more_than_50_hashtags")
        .as("number_hashtags")
    )

    val df_final = df_final_2.groupBy(
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

    df_final.show(50)
    writeCSV(df_final, ",", args(1), "profile_post_impact")


    //Time calculation ends
    val t_fin = System.nanoTime()
    val duration = Duration(t_fin - t_ini, NANOSECONDS)
    println("Elapsed time is: " + duration.toSeconds + " seconds")
  }
}