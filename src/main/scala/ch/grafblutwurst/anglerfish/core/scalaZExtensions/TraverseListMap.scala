package ch.grafblutwurst.anglerfish.core.scalaZExtensions

import scalaz._
import Scalaz._

import scala.collection.immutable.ListMap

object TraverseListMap {

  implicit def traverseListMap[K] = new Traverse[ListMap[K, ?]] {
    override def traverseImpl[G[_], A, B](fa: ListMap[K, A])(f: A => G[B])(implicit G: Applicative[G]): G[ListMap[K, B]] =
      fa.foldLeft(
        G.pure(ListMap.empty[K,B])
      )(
        (gListMapB, ka) => ^^(gListMapB, G.pure(ka._1), f(ka._2))((listMapB, k, b) => listMapB + (k -> b))
      )
  }

}
