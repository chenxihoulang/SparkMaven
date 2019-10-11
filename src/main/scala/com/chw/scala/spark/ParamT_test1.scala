package com.chw.scala.spark

object ParamT_test1 {
  def main(args: Array[String]): Unit = {
    //    val q = new Queue(List(1, 2, 3), Nil)
    //    val q1 = q enqueue 4
    //    val p = q1 head
    //    println(q)


    //        val q = Queue(1,2,3)
    //        val q3 = q enqueue 4

    val q1 = Queue1(1, 2, 3)
  }
}

trait Queue1[T] {
  def head: T

  def tail: Queue1[T]

  def enqueue(x: T): Queue1[T]
}

object Queue1 {
  def apply[T](xs: T*): Queue1[T] = new QueueImpl[T](xs.toList, Nil)

  private class QueueImpl[T](private val leading: List[T],
                             private val trailing: List[T]) extends Queue1[T] {
    private def mirror =
      if (leading.isEmpty)
        new QueueImpl(trailing.reverse, Nil)
      else
        this

    def head = mirror.leading.head

    def tail = {
      val q = mirror
      new QueueImpl(q.leading.tail, q.trailing)
    }

    def enqueue(x: T) =
      new QueueImpl(leading, x :: trailing)
  }

}

object Queue {
  def apply[T](xs: T*) = new Queue[T](xs.toList, Nil)
}

class Queue[T] private(private val leading: List[T],
                       private val trailing: List[T]) {

//  def this() = this(Nil, Nil)
//
//  def this(elems: T*) = this(elems.toList, Nil)

  private def mirror =
    if (leading.isEmpty)
      new Queue(trailing.reverse, Nil)
    else
      this

  def head = mirror.leading.head

  def tail = {
    val q = mirror
    new Queue(q.leading.tail, q.trailing)
  }

  def enqueue(x: T) =
    new Queue(leading, x :: trailing)
}