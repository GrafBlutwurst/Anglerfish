package ch.grafblutwurst.anglerfish.core.stdLibExtensions

import scala.collection.immutable.ListMap

object ListSyntax {

  implicit class KVListSyntax[K,V](lst:List[(K,V)]) {
    def toListMap():ListMap[K,V] = lst.foldLeft(ListMap.empty[K,V])(
      (map, kv) => map + kv
    )
  }

}
