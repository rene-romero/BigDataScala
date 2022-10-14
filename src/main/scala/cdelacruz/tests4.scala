package cdelacruz

object tests4 extends App {
  lazy val geek = {

    println("Initialization for the first time")
    12
  }
  // Part 1
  println(geek)

  // Part 2
  print(geek)
}
