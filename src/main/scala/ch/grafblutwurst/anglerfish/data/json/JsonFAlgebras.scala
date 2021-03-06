package ch.grafblutwurst.anglerfish.data.json

import matryoshka._
import matryoshka.implicits._
import scalaz._
import Scalaz._
import JsonData._
import io.circe.parser._
import io.circe._
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.MonadErrorSyntax._
import ch.grafblutwurst.anglerfish.data.json.implicits._

import scala.collection.immutable.ListMap



object JsonFAlgebras {

  private def asM[M[_], T: Decoder](cursor:ACursor)(implicit M: MonadError[M, Throwable]):M[T] = M.fromEither(cursor.as[T])


  def unfoldJsonFromCursor[M[_], F[_[_]]](implicit M:MonadError[M, Throwable]):CoalgebraM[M, JsonF, ACursor] = {
    case cursor:ACursor if cursor.focus.exists(_.isNull) => M.pure(JsonFNull())
    case cursor:ACursor if cursor.focus.exists(_.isBoolean) => asM[M, Boolean](cursor).map(x => if (x) JsonFTrue() else JsonFFalse())
    case cursor:ACursor if cursor.focus.exists(_.isNumber) => for {
      jsonNumber <- asM[M, JsonNumber](cursor)

      optByte = jsonNumber.toByte.map(JsonData.JsonFNumberByte[ACursor])
      optShort = jsonNumber.toShort.map(JsonData.JsonFNumberShort[ACursor])
      optInt = jsonNumber.toInt.map(JsonData.JsonFNumberInt[ACursor])
      optLong = jsonNumber.toLong.map(JsonData.JsonFNumberLong[ACursor])
      optBigInt = jsonNumber.toBigInt.map(JsonData.JsonFNumberBigInt[ACursor])


      optFloatingPoint = jsonNumber.toBigDecimal.map(
        x => if (x < Double.MaxValue && x > Double.MinValue) JsonData.JsonFNumberDouble[ACursor](x.toDouble) else JsonData.JsonFNumberBigDecimal[ACursor](x)
      )

      elementOpt =
        optByte.map(x => x:JsonF[ACursor]) <+>
        optShort.map(x => x:JsonF[ACursor]) <+>
        optInt.map(x => x:JsonF[ACursor]) <+>
        optLong.map(x => x:JsonF[ACursor]) <+>
        optBigInt.map(x => x:JsonF[ACursor]) <+>
        optFloatingPoint.map(x => x:JsonF[ACursor])

      element <- elementOpt.map(x => M.pure(x)).getOrElse(M.raiseError[JsonF[ACursor]](new RuntimeException("Could not find smallest numeric type for cursor")))

    } yield element
    case cursor:ACursor if cursor.focus.exists(_.isArray) => for {
      values <- M.fromEither(cursor.values.toRight(DecodingFailure("Was Not actually an Array", cursor.history)))
      elem = JsonFArray(values.map(_.hcursor:ACursor).toList)
    } yield elem
    case cursor:ACursor if cursor.focus.exists(_.isObject) => for {
      fieldNames <- M.fromEither(cursor.fields.toRight(DecodingFailure("Was Not actually an Array", cursor.history)))
      properties = fieldNames.foldLeft(ListMap.empty[String, ACursor])((map, fieldName) => map + (fieldName -> cursor.downField(fieldName)))
      elem = JsonFObject(properties)
    } yield elem
    case cursor:ACursor if cursor.focus.exists(_.isString) => asM[M, String](cursor).map(JsonFString.apply)
  }


  def parseJsonF[M[_], F[_[_]]](jsonString:String)(implicit M:MonadError[M, Throwable], corec:Corecursive.Aux[F[JsonF], JsonF]) = for {
    cursor <- M.fromEither(parse(jsonString)).map(_.hcursor:ACursor)
    jsonF <- cursor.anaM[F[JsonF]][M, JsonF](unfoldJsonFromCursor[M, F])
  } yield jsonF

}
