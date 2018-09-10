package ch.grafblutwurst.anglerfish.data.json

import scalaz._
import Scalaz._
import JsonData._
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.TraverseListMap._


object implicits {
  
  
  implicit val jsonFTraverse = new Traverse[JsonF] {
    override def traverseImpl[G[_], A, B](fa: JsonF[A])(f: A => G[B])(implicit G: Applicative[G]): G[JsonF[B]] = fa match {
      case json:JsonFNull[A]         => G.pure(JsonFNull[B]())
      case json:JsonFTrue[A]         => G.pure(JsonFTrue[B]())
      case json:JsonFFalse[A]        => G.pure(JsonFFalse[B]())
      case json:JsonFNumberBigDecimal[A] => G.pure(JsonFNumberBigDecimal[B](json.value))
      case json:JsonFNumberDouble[A] => G.pure(JsonFNumberDouble[B](json.value))
      case json:JsonFNumberBigInt[A] => G.pure(JsonFNumberBigInt[B](json.value))
      case json:JsonFNumberLong[A] => G.pure(JsonFNumberLong[B](json.value))
      case json:JsonFNumberInt[A]    => G.pure(JsonFNumberInt[B](json.value))
      case json:JsonFNumberShort[A] => G.pure(JsonFNumberShort[B](json.value))
      case json:JsonFNumberByte[A] => G.pure(JsonFNumberByte[B](json.value))
      case json:JsonFString[A]       => G.pure(JsonFString[B](json.value))
      case json:JsonFArray[A]        => G.map(json.values.traverse(f))(JsonFArray[B](_))
      case json:JsonFObject[A]       => G.map(json.fields.traverse(f))(JsonFObject[B](_))
    }
  }  

}
