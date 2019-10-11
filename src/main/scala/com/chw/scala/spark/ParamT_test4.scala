package com.chw.scala.spark

object ParamT_test4 {
  def main(args: Array[String]): Unit = {
    val q3 = new Queue3[Parent]

    val parent: Parent = q3.enqueue(new Parent)
    val parent1: Parent = q3.enqueue(new Child)
    val parent2: GrandParent = q3.enqueue(new GrandParent)

    val ab = new Person("a", "b")
    val ac = new Person("a", "c")
    val bc = new Person("b", "c")
    val ba = new Person("b", "a")

    println(ab < ac)
    println(bc > ab)

    val listPerson = List(ab, ac, bc, ba)
    println(orderedMergeSort(listPerson))
  }

  /**
   * 归并排序,要求T是Ordered的子类型
   */
  def orderedMergeSort[T <: Ordered[T]](xs: List[T]): List[T] = {
    def merge(xs: List[T], ys: List[T]): List[T] =
      (xs, ys) match {
        case (Nil, _) => ys
        case (_, Nil) => xs
        case (::(x, xs1), y :: ys1) =>
          if (x < y) x :: merge(xs1, ys)
          else y :: merge(xs, ys1)
      }

    val n = xs.length / 2
    if (n == 0) xs
    else {
      val (ys, zs) = xs splitAt n
      merge(orderedMergeSort(ys), orderedMergeSort(zs))
    }
  }
}

class Queue3[+T]() {
  /**
   * 语法结构 U >: T ，定义 T 为 U 的下界，因此U必须是 T 的一个父类或者T自己。
   */
  def enqueue[U >: T](x: U): U = x
}


class Person(val firstName: String, val lastName: String) extends Ordered[Person] {
  def compare(that: Person) = {
    val lastNameComparison =
      lastName.compareToIgnoreCase(that.lastName)
    if (lastNameComparison != 0)
      lastNameComparison
    else
      firstName.compareToIgnoreCase(that.firstName)
  }

  override def toString = s"${(firstName, lastName)}"
}

