package ch.grafblutwurst.anglerfish.data.json

import scala.collection.immutable.ListMap

object JsonData {

  sealed trait JsonF[A]
  final case class JsonFNull[A]() extends JsonF[A]
  final case class JsonFTrue[A]() extends JsonF[A]
  final case class JsonFFalse[A]() extends JsonF[A]
  final case class JsonFArray[A](values:List[A]) extends JsonF[A]
  final case class JsonFObject[A](fields:ListMap[String, A]) extends JsonF[A]
  final case class JsonFNumberDouble[A](value:Double) extends JsonF[A]
  final case class JsonFNumberInt[A](value:Long) extends JsonF[A]
  final case class JsonFString[A](value:String) extends JsonF[A]

}
