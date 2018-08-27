package ch.grafblutwurst.anglerfish.data.avro

import eu.timepit.refined._
import eu.timepit.refined.numeric._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.api.Validate
import eu.timepit.refined.string._
import eu.timepit.refined.collection._
import matryoshka.data.Nu

import scala.collection.immutable.{ListMap, ListSet}


object AvroData{

  //Required Refinements
  final case class AvroValidUnion()

  /**
    * This makes sure that a List of AvroTypes actually forms a valid Union as defined in
    *  https://avro.apache.org/docs/1.8.1/spec.html#Unions
  **/
 /* implicit def validateUnionMembers[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]): Validate.Plain[List[F[AvroType]], AvroValidUnion] = //TODO: Figure out how to apply this
    Validate.fromPredicate(
      lstF =>{
        val lst = lstF.map(birec.project(_))
        lst.filter{ case AvroUnionType(_) => true }.length == 0 && // avro unions may not contain any other unions directly
        lst.map{ 
          case AvroNullType() => 1
          case AvroBooleanType() => 2
          case AvroIntType() => 3
          case AvroLongType() => 4
          case AvroFloatType() => 5
          case AvroDoubleType() => 6
          case AvroBytesType() => 7
          case AvroStringType() => 8
          case _: AvroMapType[_] => 9
          case _: AvroArrayType[_] => 10
          case _ => 0
        }
          .filter( _> 0)
          .groupBy(identity)
          .forall(_._2.length == 1) && // make sure there are no non-named avro typed double in the union
        lst.map{ 
          case rec: AvroRecordType[_] => (rec.namespace, rec.name)
          case enum: AvroEnumType[_] => (enum.namespace, enum.name)
          case fixed:AvroFixedType[_] => (fixed.namespace, fixed.name)
          case _ => ("", "")
        }
          .filter( tp => tp._1 != "" && tp._2 != "")
          .groupBy(identity)
          .forall(_._2 == 1) // make sure that named members (enums, fixed and records) do not appear more than once
      },
      lst => s"$lst is a valid list of union members", 
      AvroValidUnion()
    )*/

  //Avro Types required to represent Schemata
  //TODO: extend with logical types and arbitraty properties


  type AvroValidName = MatchesRegex[W.`"[A-Za-z_][A-Za-z0-9_]*"`.T]

  final case class AvroValidNamespace()
  implicit val validateNamespace: Validate.Plain[String, AvroValidNamespace] =
    Validate.fromPredicate(
      s => {
        s.split("\\.").forall(_.matches("[A-Za-z_][A-Za-z0-9_]*")) && !s.startsWith(".") && !s.endsWith(".")
      },
      s => s"$s is a valid namespace", 
      AvroValidNamespace()
    )


  type AvroName = String Refined AvroValidName
  type AvroNamespace = String Refined AvroValidNamespace
  type AvroFQN = AvroNamespace //Has the same Refinement Type as namespace but is actually Namespace + Name
  type OptionalNonEmptySet[A] = Option[Set[A] Refined NonEmpty]

  sealed trait AvroType[A]

  sealed trait AvroPrimitiveType[A] extends AvroType[A]
  final case class AvroNullType[A]() extends AvroPrimitiveType[A]
  final case class AvroBooleanType[A]() extends AvroPrimitiveType[A]
  final case class AvroIntType[A]() extends AvroPrimitiveType[A]
  final case class AvroLongType[A]() extends AvroPrimitiveType[A]
  final case class AvroFloatType[A]() extends AvroPrimitiveType[A]
  final case class AvroDoubleType[A]() extends AvroPrimitiveType[A]
  final case class AvroBytesType[A]() extends AvroPrimitiveType[A]
  final case class AvroStringType[A]() extends AvroPrimitiveType[A]


  sealed trait AvroComplexType[A] extends AvroType[A]
  final case class AvroRecordType[A](namespace:Option[AvroNamespace], name:AvroName, doc:Option[String], aliases:OptionalNonEmptySet[AvroFQN], fields:ListMap[AvroRecordFieldMetaData, A]) extends AvroComplexType[A]
  final case class AvroEnumType[A](namespace:Option[AvroNamespace], name:AvroName, doc:Option[String], aliases:OptionalNonEmptySet[AvroFQN], symbols:ListSet[AvroName] Refined NonEmpty) extends AvroComplexType[A]
  final case class AvroArrayType[A](items:A) extends AvroComplexType[A]
  final case class AvroMapType[A](values:A) extends AvroComplexType[A]
  final case class AvroUnionType[A](members:List[A]) extends AvroComplexType[A]
  final case class AvroFixedType[A](namespace: Option[AvroNamespace]  , name:AvroName, doc:Option[String], aliases:OptionalNonEmptySet[AvroFQN], length:Int Refined Positive) extends AvroComplexType[A]
  final case class AvroRecursionType[A](fqn:String, lazyType: A) extends AvroComplexType[A]


  final case class AvroRecordFieldMetaData(name:String, doc:Option[String], default:Option[String], order:AvroRecordSortOrder, aliases:OptionalNonEmptySet[AvroFQN]) //FIXME: default should somehow have something to do with the Avro type? does Default work for complex types? e.g. a field that is itself a records? if so how is it represented? JSON encoding? In schema it's a JSON Node. Evaluating that might require the recursive Datatype for instances we still have to do

  //helpers
  sealed trait AvroRecordSortOrder
  final case object ARSOAscending extends AvroRecordSortOrder
  final case object ARSODescending extends AvroRecordSortOrder
  final case object ARSOIgnore extends AvroRecordSortOrder



  sealed trait AvroValue[S, A]

  sealed trait AvroPrimitiveValue[S, A] extends AvroValue[S, A]
  final case class AvroNullValue[S, A](schema:AvroNullType[S]) extends AvroPrimitiveValue[S, A]
  final case class AvroBooleanValue[S, A](schema:AvroBooleanType[S], value:Boolean) extends AvroPrimitiveValue[S, A]
  final case class AvroIntValue[S, A](schema:AvroIntType[S], value:Int) extends AvroPrimitiveValue[S, A]
  final case class AvroLongValue[S, A](schema:AvroLongType[S], value:Long) extends AvroPrimitiveValue[S, A]
  final case class AvroFloatValue[S, A](schema:AvroFloatType[S], value:Float) extends AvroPrimitiveValue[S, A]
  final case class AvroDoubleValue[S, A](schema:AvroDoubleType[S], value:Double) extends AvroPrimitiveValue[S, A]
  final case class AvroBytesValue[S, A](schema:AvroBytesType[S], value: Vector[Byte]) extends AvroPrimitiveValue[S, A]
  final case class AvroStringValue[S, A](schema:AvroStringType[S], value: String) extends AvroPrimitiveValue[S, A]

  sealed trait AvroComplexValue[S, A] extends AvroValue[S, A]
  final case class AvroRecordValue[S, A](schema:AvroRecordType[S], fields:ListMap[String, A]) extends AvroComplexValue[S, A] 
  final case class AvroEnumValue[S, A](schema:AvroEnumType[S], symbol:String) extends AvroComplexValue[S, A]
  final case class AvroArrayValue[S, A](schema:AvroArrayType[S], items:List[A]) extends AvroComplexValue[S, A]
  final case class AvroMapValue[S, A](schema:AvroMapType[S], values:Map[String, A]) extends AvroComplexValue[S, A]
  final case class AvroUnionValue[S, A](schema:AvroUnionType[S], member:A) extends AvroComplexValue[S, A]
  final case class AvroFixedValue[S, A](schema:AvroFixedType[S], bytes:Vector[Byte]) extends AvroComplexValue[S, A]





  sealed trait AvroSchema
  final case class AvroSchemaReference(name:String) extends AvroSchema
  final case class AvroSchemaUnion() extends AvroSchema
  final case class AvroSchemaType[A](aType:A) extends AvroSchema

}
