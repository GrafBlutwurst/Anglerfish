package ch.grafblutwurst.anglerfish.data.json

import scala.collection.immutable.ListMap

object JsonData {

  sealed trait JsonF[A]
  final case class JsonFNull[A]() extends JsonF[A]
  final case class JsonFTrue[A]() extends JsonF[A]
  final case class JsonFFalse[A]() extends JsonF[A]
  final case class JsonFArray[A](values:List[A]) extends JsonF[A]
  final case class JsonFObject[A](fields:ListMap[String, A]) extends JsonF[A]
  final case class JsonFNumberBigDecimal[A](value:BigDecimal) extends JsonF[A] //How much of a footgun is BigDecimal
  final case class JsonFNumberDouble[A](value:Double) extends JsonF[A]
  final case class JsonFNumberBigInt[A](value:BigInt) extends JsonF[A]
  final case class JsonFNumberLong[A](value:Long) extends JsonF[A]
  final case class JsonFNumberInt[A](value:Int) extends JsonF[A]
  final case class JsonFNumberShort[A](value:Short) extends JsonF[A]
  final case class JsonFNumberByte[A](value:Byte) extends JsonF[A]
  final case class JsonFString[A](value:String) extends JsonF[A]


  object JsonF {
    def flatDescribe(jsonF: JsonF[_]):String = jsonF match {
      case JsonFArray(values) => s"JsonFArray(length = ${values.length}" 
      case JsonFObject(fields) => s"JsonFObject(fieldNames = ${fields.keys.mkString(",")})"
      case x:JsonF[_] => x.toString
    }
  }

}
