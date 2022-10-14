package cdelacruz

import org.apache.hadoop.fs.Options.HandleOpt.path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object tests3 extends App {
  val folder_file = "src/main/resources/single_file"
  val format_file = "parquet"
  val new_name = "instagram"
  val directory = new File(folder_file)

  if (directory.exists && directory.isDirectory) {
    val file = directory.listFiles.filter(_.getName.endsWith(new_name+"." + format_file))
    if(file.size > 0) {
      println(file.size+"_"+new_name)
    }
    else {
      println(new_name)
    }
  }
}
