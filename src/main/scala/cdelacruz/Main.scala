package cdelacruz

import Functions._
import org.apache.spark.sql.functions._
import ReadData._
import org.apache.spark.sql.types.{DecimalType, LongType, StructField, StructType, StringType, TimestampType, IntegerType}

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

    val postsSchema = StructType(Array(
      StructField("sid", LongType),
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("street", LongType),
      StructField("zip", LongType),
      StructField("city", TimestampType),
      StructField("region", IntegerType),
      StructField("cd", StringType),
      StructField("numbr_likes", LongType),
      StructField("number_comments", LongType)
    ))

    val df = my_spark.read
      .format("csv")
      .option("mode", "PERMISSIVE")
      .option("header", "true")
      .option("delimiter", "\t")
      .load(args(1))

    val df_final = df.groupBy(
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

    df_final.show(50)
    writeCSV(df_final, ",", args(1), "most_visited_cities")


    //Time calculation ends
    val t_fin = System.nanoTime()
    val duration = Duration(t_fin - t_ini, NANOSECONDS)
    println("Elapsed time is: " + duration.toSeconds + " seconds")
  }
}