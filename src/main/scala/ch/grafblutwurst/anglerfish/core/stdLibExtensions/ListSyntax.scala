package ch.grafblutwurst.anglerfish.core.stdLibExtensions

import scala.collection.immutable.{ListMap, ListSet}

object ListSyntax {

  implicit class KVListSyntax[K,V](lst:List[(K,V)]) {
    def toListMap():ListMap[K,V] = lst.foldLeft(ListMap.empty[K,V])(_ + _)
  }

  implicit class ListSyntax[T](lst:List[T]) {
    def toListSet():ListSet[T] = lst.foldLeft(ListSet.empty[T])(_ + _)
  }

}
