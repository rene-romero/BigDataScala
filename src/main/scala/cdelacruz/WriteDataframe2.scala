package cdelacruz

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataframe2 extends App{
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExample")
    .getOrCreate()

  val df_posts = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter", "\t")
    .load("src/main/scala/instagram_posts_original.csv")

  val df_locations = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter", "\t")
    .load("src/main/scala/instagram_locations.csv")
    .dropDuplicates("id")

  val df_profiles = spark.read
    .format("csv")
    .option("header","true")
    .option("delimiter", "\t")
    .load("src/main/scala/instagram_profiles.csv")
    .dropDuplicates("profile_id")

  df_profiles.createTempView("profiles")
  df_locations.createTempView("locations")
  df_posts.createTempView("posts")

  val df_insta = spark.sql(
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
              ,c.numbr_likes
              ,c.number_comments
      from profiles a
      left join locations b on a.profile_id = b.id
      inner join posts c on a.profile_id = c.profile_id
      """.stripMargin)

  //repartition was used to make only 1 file csv.
  df_insta.repartition(1).write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("delimiter", "\t")
    .option("header", "true")
    .save("src/main/scala/df")
}
