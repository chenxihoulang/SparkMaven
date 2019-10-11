package com.chw.scala.spark

/**
 * Covariance（协变关系）和 Contravariance(逆变关系），分别以 Queue[+T]和 Queue[-T] 代表。
 */
object ParamT_test2 {
  def main(args: Array[String]): Unit = {
    foo(new Box[Parent])
    foo(new Box[Child])
    //错误
    //    foo(new Box[GrandParent])

    bar(new Box2[Parent])
    //错误
    //    bar(new Box2[Child])
    bar(new Box2[GrandParent])

    doo(new Box3[Parent])
    //错误
    //    doo(new Box3[Child])
    //错误
    //    doo(new Box3[GrandParent])
  }

  def foo(x: Box[Parent]): Box[Parent] = identity(x)

  def bar(x: Box2[Parent]): Box2[Parent] = identity(x)

  def doo(x: Box3[Parent]): Box3[Parent] = identity(x)
}

class GrandParent {
  def d1(): Unit = {
    println("GrandParent")
  }
}

class Parent extends GrandParent {
  def d2(): Unit = {
    println("Parent")
  }
}

class Child extends Parent {
  def d3(): Unit = {
    println("Child")
  }
}

/**
 * Covariance（协变关系）是在类型前面使用“+”号，表示如果两个类型 T，S 如果 T 是 S 的子类，那么 Queue[T]也是 Queue[S]的子类
 * 参数类型只能出现在输出位置
 */
class Box[+A]() {
  def output(): Option[A] = {
    None
  }
}

/**
 * Contravariable（逆变关系）则相反，如果如果两个类型 T，S 如果 T 是 S 的子类，反过来，Queue[S]是 Queue[T]的子类。
 * 参数类型只能出现在输入位置
 */
class Box2[-A] {
  def input(a: A) {

  }
}

/**
 * 参数类型可出现在任意位置,缺省情况比如 Box3[T]，Box3[String]和 Box3[AnyRef]不存在继承关系。
 *
 * @tparam A
 */
class Box3[A] {
  def inputAndOutput(a: A): A = {
    a
  }
}