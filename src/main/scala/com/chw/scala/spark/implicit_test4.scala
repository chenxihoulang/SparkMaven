package com.chw.scala.spark

object implicit_test4 {
  def main(args: Array[String]): Unit = {

    maxListImpParam(List(1, 5, 10, 34, 23))
    maxListImpParam(List(3.4, 5.6, 23, 1.2))
    maxListImpParam(List("one", "two", "three"))

    //运行错误
    //    maxListUpBound(List(1, 2, 3, 0, 100))

    maxList(List(1, 2, 3, 4, 5))
  }


  /**
   * 这个函数是求取一个顺序列表的最大值。但这个函数有个局限，它要求类型 T 是 Ordered[T]的一个子类，
   * 因此这个函数无法求一个整数列表的最大值。
   */
  def maxListUpBound[T <: Ordered[T]](element: List[T]): T =
    element match {
      case List() =>
        throw new IllegalArgumentException("empty list!")
      case List(x) => x
      case x :: rest =>
        val maxRest = maxListUpBound(rest)
        if (x > maxRest) x
        else maxRest
    }

  /**
   * 下面我们使用隐含参数来解决这个问题。 我们可以再定义一个隐含参数，其类型为一函数类型，可以把一个类型T转换成 Ordered[T]。
   */
  def maxListImpParam[T](element: List[T])
                        (implicit orderer: T => Ordered[T]): T =
    element match {
      case List() =>
        throw new IllegalArgumentException("empty list!")
      case List(x) => x
      case x :: rest =>
        val maxRest = maxListImpParam(rest)(orderer)
        if (orderer(x) > maxRest) x
        else maxRest
    }

  /**
   * 当在参数中使用 implicit 类型时，编译器不仅仅在需要时补充隐含参数，
   * 而且编译器也会把这个隐含参数作为一个当前作用域内可以使用的隐含变量使用，
   * 因此在使用隐含参数的函数体内可以省略掉 implicit 的调用而由编译器自动补上。
   */
  def maxList[T](element: List[T])
                (implicit orderer: T => Ordered[T]): T =
    element match {
      case List() =>
        throw new IllegalArgumentException("empty list!")
      case List(x) => x
      case x :: rest =>
        val maxRest = maxList(rest)
        if (x > maxRest) x
        else maxRest
    }

  /**
   * 其中 <% 为 View 限定，也就是说，我可以使用任意类型的 T，只要它可以看成类型 Ordered[T]。
   * 这和 T 是 Orderer[T]的子类不同，它不需要 T 和 Orderer[T]之间存在继承关系。
   * 而如果类型 T 正好也是一个 Ordered[T]类型，你也可以直接把 List[T]传给 maxList，此时编译器使用一个恒等隐含变换:
   * implicit def identity[A](x:A): A =x
   */
  def maxList1[T <% Ordered[T]](element: List[T]): T =
    element match {
      case List() =>
        throw new IllegalArgumentException("empty list!")
      case List(x) => x
      case x :: rest =>
        val maxRest = maxList(rest)
        if (x > maxRest) x
        else maxRest
    }
}
