package cdelacruz

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataframe extends App {
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExample")
    .getOrCreate()

  val df_posts = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter", "\t")
    .load("src/main/scala/instagram_posts_original.csv")

  val df_posts_limit = df_posts.distinct().limit(15000000)

  df_posts_limit.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("delimiter", "\t")
    .option("header", "true")
    .save("src/main/scala/df")
}
