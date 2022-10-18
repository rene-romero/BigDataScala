package cdelacruz

import Functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SaveMode}

object ReadData extends App {

  val postsSchema = StructType(Array(
    StructField("sid",LongType),
    StructField("sid_profile",LongType),
    StructField("post_id",StringType),
    StructField("profile_id",LongType),
    StructField("location_id",LongType),
    StructField("cts",TimestampType),
    StructField("post_type",IntegerType),
    StructField("description",StringType),
    StructField("numbr_likes",LongType),
    StructField("number_comments",LongType)
  ))

  val df_posts = my_spark.read
    .format("csv")
    .schema(postsSchema)
    .option("mode", "PERMISSIVE")
    .option("header", "true")
    .option("delimiter", "\t")
    .load("src/main/resources/dataset/instagram_posts.csv")

  val df_locations = my_spark.read
    .format("csv")
    .option("mode","PERMISSIVE")
    .option("header", "true")
    .option("delimiter", "\t")
    .load("src/main/resources/dataset/instagram_locations.csv")

  val df_profiles = my_spark.read
    .format("csv")
    .option("mode","PERMISSIVE")
    .option("header", "true")
    .option("delimiter", "\t")
    .load("src/main/resources/dataset/instagram_profiles.csv")
    .dropDuplicates("profile_id")

  df_profiles.createTempView("profiles")
  df_locations.createTempView("locations")
  df_posts.createTempView("posts")

  val df = my_spark.sql(
    """
      select  a.profile_id
              ,a.profile_name
              ,a.firstname_lastname
              ,a.description
              ,a.following
              ,a.followers
              ,a.url
              ,a.cts timestamp_location
              ,a.is_business_account
              ,b.name location_name
              ,b.street
              ,b.zip
              ,b.city
              ,b.region
              ,b.cd country_code
              ,b.phone
              ,b.lat latitude
              ,b.lng longitude
              ,b.website
              ,c.post_id
              ,c.cts timestamp_post
              ,c.post_type
              ,c.description description_post
              ,c.numbr_likes number_likes
              ,c.number_comments
      from profiles a
      left join locations b on a.profile_id = b.id
      left join posts c on a.profile_id = c.profile_id
      """.stripMargin)

  //repartition was used to make only 1 file csv.
  /*df_insta.repartition(1).write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("delimiter", "\t")
    .option("header", "true")
    .save("src/main/resources/single_file")*/

  //Alternatively I was able to cast my fields with de previous query SQL.
  val df_insta = df.select(
    col("profile_id").cast(LongType),
    col("profile_name").cast(StringType),
    col("description").cast(StringType),
    col("following").cast(StringType),
    col("followers").cast(LongType),
    col("url").cast(StringType),
    col("timestamp_location").cast(TimestampType),
    col("is_business_account").cast(BooleanType),
    col("location_name").cast(StringType),
    col("street").cast(StringType),
    col("zip").cast(StringType),
    col("city").cast(StringType),
    col("region").cast(StringType),
    col("country_code").cast(StringType),
    col("phone").cast(StringType),
    col("latitude").cast(DecimalType(8,6)),
    col("longitude").cast(DecimalType(9,6)),
    col("website").cast(StringType),
    col("post_id").cast(StringType),
    col("timestamp_post").cast(TimestampType),
    col("post_type").cast(IntegerType),
    col("description_post").cast(StringType),
    col("number_likes").cast(LongType),
    col("number_comments").cast(LongType)
  )

  df_insta.coalesce(1).write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/single_file")

  renameFile("src/main/resources/single_file","parquet","instagram")
}