package cdelacruz

import java.io.File

object tests5 extends App {
  val directory = new File("src/main/resources/single_file")
  val format_file = "csv"

  if (directory.exists && directory.isDirectory) {
    val file = directory.listFiles.filter(_.getName.endsWith("." + format_file)).head
    println("res:")
    println(file)
  }
}
