package cdelacruz

import cdelacruz.Functions.my_spark
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object tests6 extends App {
  val postsSchema = StructType(Array(
    StructField("sid", LongType),
    StructField("sid_profile", LongType),
    StructField("post_id", StringType),
    StructField("profile_id", LongType),
    StructField("location_id", LongType),
    StructField("cts", TimestampType),
    StructField("post_type", IntegerType),
    StructField("description", StringType),
    StructField("numbr_likes", LongType),
    StructField("number_comments", LongType)
  ))

  val df_posts = my_spark.read
    .format("csv")
    .schema(postsSchema)
    .option("mode", "PERMISSIVE")
    .option("header", "true")
    .option("delimiter", "\t")
    .load("src/main/resources/dataset/instagram_posts.csv")

  df_posts.show(25)
}
