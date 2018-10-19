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
import scalaz.Liskov.>~>

import scala.collection.immutable.ListMap



object JsonFAlgebras {

  sealed trait JsonError extends Throwable{
    override def getMessage: String = Show[JsonError].shows(this)
  }
  object JsonError {
    implicit val showInstance: Show[JsonError] = Show.shows[JsonError] {
      case NumericTypeError(element) => s"$element could not be matched to allowed numeric type"
      case CirceDecodingError(err) => err.getMessage()
      case CirceParsingError(err) => err.getMessage()
    }
  }
  final case class NumericTypeError(element:JsonNumber) extends JsonError
  final case class CirceDecodingError(err:DecodingFailure) extends JsonError
  final case class CirceParsingError(err:ParsingFailure) extends JsonError

  private def asM[M[_], T: Decoder, E](cursor:ACursor)(implicit M: MonadError[M, E], liskov: E >~> JsonError):M[T] = M.fromEither(cursor.as[T].left.map(df => liskov(CirceDecodingError(df))))





  def unfoldJsonFromCursor[M[_], F[_[_]], E](implicit M:MonadError[M, E], liskov: E >~> JsonError):CoalgebraM[M, JsonF, ACursor] = {
    case cursor:ACursor if cursor.focus.exists(_.isNull) => M.pure(JsonFNull())
    case cursor:ACursor if cursor.focus.exists(_.isBoolean) => asM[M, Boolean, E](cursor).map(x => if (x) JsonFTrue() else JsonFFalse())
    case cursor:ACursor if cursor.focus.exists(_.isNumber) => for {
      jsonNumber <- asM[M, JsonNumber, E](cursor)

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

      element <- elementOpt.map(x => M.pure(x)).getOrElse(M.raiseError[JsonF[ACursor]](liskov(NumericTypeError(jsonNumber))))

    } yield element
    case cursor:ACursor if cursor.focus.exists(_.isArray) => for {
      values <- M.fromEither(cursor.values.toRight(liskov(CirceDecodingError(DecodingFailure("Was Not actually an Array", cursor.history)))))
      elem = JsonFArray(values.map(_.hcursor:ACursor).toList)
    } yield elem
    case cursor:ACursor if cursor.focus.exists(_.isObject) => for {
      fieldNames <- M.fromEither(cursor.keys.toRight(liskov(CirceDecodingError(DecodingFailure("Was Not actually an Array", cursor.history)))))
      properties = fieldNames.foldLeft(ListMap.empty[String, ACursor])((map, fieldName) => map + (fieldName -> cursor.downField(fieldName)))
      elem = JsonFObject(properties)
    } yield elem
    case cursor:ACursor if cursor.focus.exists(_.isString) => asM[M, String, E](cursor).map(JsonFString.apply)
  }

  val foldToJson: Algebra[JsonF, Json] = {
    case JsonFNull() =>  Json.Null
    case JsonFTrue() => Json.True
    case JsonFFalse() => Json.False
    case JsonFNumberBigDecimal(value) => Encoder[BigDecimal].apply(value)
    case JsonFNumberDouble(value) => Encoder[Double].apply(value)
    case JsonFNumberBigInt(value) => Encoder[BigInt].apply(value)
    case JsonFNumberLong(value) => Encoder[Long].apply(value)
    case JsonFNumberInt(value) => Encoder[Int].apply(value)
    case JsonFNumberShort(value) => Encoder[Short].apply(value)
    case JsonFNumberByte(value) => Encoder[Byte].apply(value)
    case JsonFString(value) => Encoder[String].apply(value)
    case JsonFArray(values) => Json.arr(values:_*)
    case JsonFObject(properties) => Json.obj(properties.toList:_*)
  }


  def parseJsonF[M[_], F[_[_]], E](jsonString:String)(implicit M:MonadError[M, E], corec:Corecursive.Aux[F[JsonF], JsonF], liskov: E >~> JsonError) = for {
    cursor <- M.fromEither(parse(jsonString).left.map(err => liskov(CirceParsingError(err)))).map(_.hcursor:ACursor)
    jsonF <- cursor.anaM[F[JsonF]][M, JsonF](unfoldJsonFromCursor[M, F, E])
  } yield jsonF

  def toJson[F[_[_]]](jsonF:F[JsonF])(implicit rec: Recursive.Aux[F[JsonF], JsonF]):Json = rec.cata(jsonF)(foldToJson)

}
