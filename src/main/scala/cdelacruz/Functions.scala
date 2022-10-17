package cdelacruz

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io._
import java.nio.file._

object Functions extends App {

  def checkSource(source: String): Unit = {
    val file = new File(source)
    if (file.exists && file.isDirectory == false) {
      println("Ready to read...")
    }
    else if (file.exists && file.isDirectory) {
      println("The source exists but it's a directory, please choose a file.")
    }
    else {
      println("The source not exists, please check again.")
    }
    return source
  }

  def my_spark: SparkSession = {
    val spark = SparkSession.builder()
      .config("spark.master", "local[*]")
      .appName("SparkByExample")
      .getOrCreate()
    return spark
  }

  def readCSV(source: String, delimiter: String, schema: StructType = StructType(Array(
    StructField("empty", StringType)))): DataFrame = {
    val fileName = Paths.get(source).getFileName
    val extension = fileName.toString.split("\\.").last
    if(schema == StructType(Array(StructField("empty", StringType)))) {
      val df = my_spark.read
        .format(extension)
        .option("header", "true")
        .option("delimiter", delimiter)
        .load(source)
      return df
    }
    else {
      val df = my_spark.read
        .format(extension)
        .schema(schema)
        .option("delimiter", "\t")
        .load(source)
      return df
    }
  }

  def renameFile(folder_file: String, format_file: String, new_name: String): Unit = {
    val directory = new File(folder_file)

    if (directory.exists && directory.isDirectory) {
      val files = directory.listFiles.filter(_.getName.endsWith(new_name+"." + format_file))
      val file = directory.listFiles.filter(_.getName.endsWith("."+format_file)).maxBy(_.lastModified())

      if (files.size > 0) {
        file.renameTo(new File(folder_file + s"/${files.size}_$new_name.$format_file"))
        println("You have changed the name of a file:")
        println("")
        println("Old name: " + file)
        println("New name: " + folder_file + s"/${files.size}_$new_name.$format_file")
      }
      else {
        file.renameTo(new File(folder_file + s"/$new_name.$format_file"))
        println("You have changed the name of a file:")
        println("")
        println("Old name: " + file)
        println("New name: " + folder_file + s"/$new_name.$format_file")
      }
    }
  }

  def writeCSV(df: DataFrame, delimiter: String, folder: String, name_file: String): Unit = {
    df.repartition(1).write
      .format("csv")
      .mode(SaveMode.Append)
      .option("delimiter", delimiter)
      .option("header", "true")
      .save(folder)
    renameFile(folder,"csv", name_file)
  }
}