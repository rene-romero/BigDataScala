package cdelacruz
import java.io.File

object tests extends App {

  def getListOfFiles(dir: String) = {
    val source = new File(dir)
    if (source.exists && source.isDirectory && source.length > 0) {
      println("-------------")
      println("Found Files:")
      println("-------------")
      val files = source.listFiles.filter(_.isFile).toList
      for (file <- files) println(file)
    }
    else if (source.exists && source.isDirectory && source.length == 0) {
      println("The source hasn't files.")
    }
    else {
      println("The source doesn't exists.")
    }
  }

  getListOfFiles("src/main/resources/")
}
