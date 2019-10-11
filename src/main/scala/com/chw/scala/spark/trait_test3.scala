package com.chw.scala.spark

object trait_test3 {
  def main(args: Array[String]): Unit = {
    val cow = new Cow
    cow.eat(new Grass)

//    cow.eat(new Fish)
  }
}


class Food

abstract class Animal {
  type SuitableFood <: Food

  def eat(food: SuitableFood)
}

class Grass extends Food
class Fish extends Food

class Cow extends Animal {
  type SuitableFood = Grass

  override def eat(food: Grass) {}
}