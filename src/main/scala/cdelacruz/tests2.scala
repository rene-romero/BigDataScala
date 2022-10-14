package cdelacruz

object tests2 extends App {

  def printMenu: Unit = {
    println(
      """
        |1-option 1
        |2-option 2
        |3-option 3
        |0-exit
        |""".stripMargin)
  }

  var option = "0"
  do {
    print("\u001b[H")
    printMenu
    println("Put something: ")
    option = scala.io.StdIn.readLine()
    option match {
      case "1" => {
        print("option 1".getClass)
      }
      case _ => printMenu
    }
  } while (option != "0")
}