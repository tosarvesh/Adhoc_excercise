package com.bigdata.scala

object matchtest {
  def main(args: Array[String]) {
println(matchf(5))

    val alice = new Person("Alice", 25)
    val bob = new Person("Bob", 32)
    val charlie = new Person("Charlie", 32)

    for (x <- List(alice, bob, charlie)) {
      x match {
        case Person("Alice", 25) => println("Hi Alice!")
        case Person("Bob", 32) => println("Hi Bob!")
        case Person(name, age) => println(
          "Age: " + age + " year, name: " + name + "?")
      }
    }

  }

def matchf(x: Int):String=x match {
    case 1=>"one"
    case 2=>"two"
    case 3=>"three"
    case _=>"none"
}

case class Person(name: String, age: Int)


}
