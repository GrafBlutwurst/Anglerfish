package ch.grafblutwurst.anglerfish.data.avro

import java.util.Base64

import scalaz._
import Scalaz._
import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.json.JsonData._
import ch.grafblutwurst.anglerfish.data.json.JsonFAlgebras._
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import ch.grafblutwurst.anglerfish.data.json.implicits._
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.MonadErrorSyntax._
import ch.grafblutwurst.anglerfish.core.stdLibExtensions.ListSyntax._
import matryoshka.{Algebra, Birecursive, GAlgebra}
import matryoshka.implicits._
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string._
import eu.timepit.refined._
import matryoshka.data.Nu
import matryoshka.data.Fix
import ch.grafblutwurst.anglerfish.data.json.JsonData._

import scala.collection.immutable.{ListMap, ListSet}

object AvroJsonFAlgebras {

  /*TODO: Schema parser.
    could actually use a Map[String, AvroType] / String => Option[AvroType] (Maybe a StateMonad) store and pass it along during unfolding.
    Provide a default store which has references to primitive types.
    When we encounter a string in type position (top level, union member or int "type:...) we look it up in the type store
    Whenever we finsihed parsing a named type (Record, Enum, Fixed) we add it to the reference store
  */


  sealed trait AvroError extends RuntimeException

  sealed trait AvroDatumError extends AvroError
  final case class EnumError(expected:AvroEnumType[Nu[AvroType]], encountered:String) extends AvroDatumError
  final case class UnionError(expected:AvroUnionType[Nu[AvroType]], reason:String) extends AvroDatumError
  final case class UnionResolutionError(reason:String) extends AvroDatumError
  final case class ArrayError(reason:String) extends AvroDatumError
  final case class RecordError(reason:String) extends AvroDatumError
  final case class FixedError(expected:AvroFixedType[Nu[AvroType]], encounteredLength:Int) extends AvroDatumError
  final case class UnexpectedTypeError(base:JsonF[_], contextSchema: AvroType[Nu[AvroType]]) extends AvroDatumError


  sealed trait AvroSchemaErrors extends AvroError
  final case class UnexpectedJsonType(jsonElement:JsonF[_], parsingContext: AvroSchemaParsingContext) extends AvroSchemaErrors
  final case class UnkownSchemaReference(errorReference:String, knownReferences: Set[String]) extends AvroSchemaErrors
  final case class UnexpectedParsingResult() extends AvroSchemaErrors
  final case class UnknownFieldError(field:String, available:Set[String]) extends AvroSchemaErrors
  final case class RefinementError(error:String) extends AvroSchemaErrors
  final case class UnknownSortOrder(error:String) extends AvroSchemaErrors
  /*final case class UnallowedTypeIdentifier(error:String) extends AvroSchemaErrors
  final case class UnallowedJsonElement[A](jsonF:JsonF[A]) extends AvroSchemaErrors*/

  /*implicit val nullDecoder:Decoder[Null] = new Decoder[Null] {
    override def apply(c: HCursor): Result[Null] = if(c.value.isNull) Right(null) else Left(DecodingFailure("value was not null", c.history))
  }

  private def as[T: Decoder](cursor:ACursor):Either[AvroDecodingErrors, T] = cursor.as[T] match {
    case Right(x) => Right(x)
    case Left(err) => Left(UnexpectedTypeError(err))
  }

  private def asM[M[_], T: Decoder](cursor:ACursor)(implicit M: MonadError[M, Throwable]):M[T] = M.fromEither(as[T](cursor))






  private[this] def decodeBytes(s:String):Vector[Byte] = Base64.getDecoder.decode(s).toVector

  private[this] def selectUnionMemberByName[M[_],F[_[_]]](members:List[Nu[AvroType]], selector:String, cursor:ACursor)(
  implicit
  birec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
  typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
  M: MonadError[M, Throwable]
  ):M[(ACursor, Nu[AvroType])] = {
     members.map(x => typeBirec.project(x)).flatMap {
      case member: AvroBooleanType[_] =>  if(selector == "boolean") List(member) else Nil
      case member: AvroIntType[_] =>if(selector == "int") List(member) else Nil
      case member: AvroLongType[_] => if(selector == "long") List(member) else Nil
      case member: AvroFloatType[_] => if(selector == "float") List(member) else Nil
      case member: AvroDoubleType[_] => if(selector == "double") List(member) else Nil
      case member: AvroBytesType[_] => if(selector == "bytes") List(member) else Nil
      case member: AvroStringType[_] => if(selector == "string") List(member) else Nil
      case member: AvroRecordType[_] => if(selector == member.namespace.value + "." + member.name.value) List(member) else Nil
      case member: AvroEnumType[_] => if(selector == member.namespace.value + "." + member.name.value) List(member) else Nil
      case member: AvroArrayType[_] => if(selector == "array") List(member) else Nil //found out by trying against java reference impl
      case member: AvroMapType[_] => if(selector == "map") List(member) else Nil //found out by trying against java reference impl
      case member: AvroFixedType[_] => if(selector == member.namespace.value + "." + member.name.value) List(member) else Nil
      case _ : AvroUnionType[_] => Nil //cannot nest unions
      case _ : AvroNullType[_] => Nil //handled seperatly
    }
       .map(tpl => (tpl._1, typeBirec.embed(tpl._2)))
      match {
       case winner::Nil => M.pure(winner)
       case Nil => M.raiseError(UnionError("Unknown Union Branch " + selector))
       case x::xs => M.raiseError(UnionError("Could not disambiguate" + selector))
     }


  }*/


  private[this] def decodeBytes(s:String):Vector[Byte] = Base64.getDecoder.decode(s).toVector

  sealed trait AvroSchemaParsingContext
  final case class TypePosition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class RecordFieldDefinition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class RecordFieldListDefinition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class DefaultDefinition(schema:Nu[AvroType]) extends AvroSchemaParsingContext
  final case class LiteralDefinition() extends AvroSchemaParsingContext
  final case class AliasesDefinition() extends AvroSchemaParsingContext
  final case class EnumSymbolDefinition() extends AvroSchemaParsingContext



  sealed trait IntermediateResults
  final case class NullLiteral() extends IntermediateResults
  final case class BooleanLiteral(b:Boolean) extends IntermediateResults
  final case class IntLiteral(i:Int) extends IntermediateResults
  final case class DoubleLiteral(d:Double) extends IntermediateResults
  final case class StringLiteral(s:String) extends IntermediateResults
  final case class Aliases(aliases:Set[AvroFQN] Refined NonEmpty) extends IntermediateResults
  final case class EnumSymbols(symbols:ListSet[AvroName] Refined NonEmpty) extends IntermediateResults
  final case class FieldDefinition(discoveredTypes: Map[String, Nu[AvroType]], meta:AvroRecordFieldMetaData, avroType: Nu[AvroType]) extends IntermediateResults
  final case class FieldDefinitions(fields:List[FieldDefinition]) extends IntermediateResults


  //FIXME error handling
  def parseAvroSchemaAlgebra[M[_], F[_[_]]](implicit M:MonadError[M, Throwable], jsonBirec:Birecursive.Aux[F[JsonF], JsonF], typeBirec:Birecursive.Aux[Nu[AvroType], AvroType]):
    Algebra[
      JsonF,
      AvroSchemaParsingContext => M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])]
    ] = jsonF => {

        type Out = AvroSchemaParsingContext => M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])]


        def refine[A, P](a: A)(implicit validate:Validate[A, P]) = M.fromEither(refineV[P](a).left.map(RefinementError))

        def fieldAsOpt[T](fields:ListMap[String, Out])(field:String)(parsingContext: AvroSchemaParsingContext)(f: PartialFunction[M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])], M[T]]):M[Option[T]] =
          fields
            .get(field)
            .map(eval => eval(parsingContext))
            .map(
              out => if (f.isDefinedAt(out)) f(out) else M.raiseError[T](UnexpectedParsingResult())
            )
            .sequence

        def fieldAs[T](fields:ListMap[String, Out])(field:String)(parsingContext: AvroSchemaParsingContext)(f: PartialFunction[M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])], M[T]]):M[T] =
          fieldAsOpt[T](fields)(field)(parsingContext)(f).flatMap {
            case Some(x) => M.pure(x)
            case None => M.raiseError[T](UnknownFieldError(field, fields.keySet))
          }


        jsonF match {
          case json:JsonFNull[Out]         =>  {
            case LiteralDefinition() => -\/(M.pure(NullLiteral()))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFTrue[Out]         =>  {
            case LiteralDefinition() => -\/(M.pure(BooleanLiteral(true)))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFFalse[Out]        =>  {
            case LiteralDefinition() => -\/(M.pure(BooleanLiteral(false)))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFNumberDouble[Out] =>  {
            case LiteralDefinition() => -\/(M.pure(DoubleLiteral(json.value)))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFNumberInt[Out]    =>  {
            case LiteralDefinition() => -\/(M.pure(IntLiteral(json.value)))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFString[Out]       =>  {
            case TypePosition(parents, refs)    => {
              \/-(

                  refs.get(json.value)
                  .map(avroType => if (parents.contains(json.value)) typeBirec.embed(AvroRecursionType(json.value, avroType)) else avroType) match {
                    case Some(x) => M.pure[(Map[String, Nu[AvroType]], Nu[AvroType])]((Map.empty[String, Nu[AvroType]], x))
                    case None => M.raiseError[(Map[String, Nu[AvroType]], Nu[AvroType])](UnkownSchemaReference(json.value, refs.keys.toSet))
                  }

              )
            }
            case LiteralDefinition() => -\/(M.pure(StringLiteral(json.value)))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          // Can be a Union if in type position, or a list of fields if in the fields property
          case json:JsonFArray[Out] => {
            case TypePosition(parents, refs) => {
              val evaluated = json.values
                .map(f => f(TypePosition(parents, refs)))
                .traverse {
                  case -\/(_) => M.raiseError[(Map[String, Nu[AvroType]], Nu[AvroType])](UnexpectedParsingResult())
                  case \/-(m) => m
                }
                .map(
                  memberTpls => {
                    val (discovered, members) = memberTpls.unzip
                    (discovered.foldLeft(Map.empty[String, Nu[AvroType]])((mapA, map) => mapA ++ map), typeBirec.embed(AvroUnionType(members)))
                  }
                )

              \/-(evaluated)
            }
            case RecordFieldListDefinition(parents, refs) => -\/(
              json.values.foldLeft(
                M.pure((Map.empty[String, Nu[AvroType]], List.empty[FieldDefinition]))
              )(
                (mOuter, f) => mOuter.flatMap(
                  tpl => {
                    f(RecordFieldDefinition(parents, refs ++ tpl._1)) match {
                      case -\/(m) => m.flatMap{
                        case f: FieldDefinition => M.pure((tpl._1 ++ f.discoveredTypes, tpl._2 ::: List(f)))
                        case _ => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](UnexpectedParsingResult())
                      }
                      case -\/(_) => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](UnexpectedParsingResult())
                      case \/-(_) => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](UnexpectedParsingResult())
                    }
                  }
                )

              ).map(
                tpl => FieldDefinitions(tpl._2)
              )
            )

            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFObject[Out] => {
            case TypePosition(parents, refs) => {
                val avroType = for {
                  typeString <- fieldAs(json.fields)("type")(LiteralDefinition()) {
                    case -\/(m) => m.flatMap {
                      case StringLiteral(s) => M.pure(s)
                      case _  => M.raiseError[String](UnexpectedParsingResult())
                    }
                  }

                  discoveryTypeTuple <- typeString match {
                    case "array" =>
                      fieldAs(json.fields)("items")(TypePosition(parents, refs)){
                        case \/-(m) => m.map(tpl => (tpl._1, AvroArrayType(tpl._2):AvroType[Nu[AvroType]]))
                      }
                    case "map" =>
                      fieldAs(json.fields)("values")(TypePosition(parents, refs)){
                        case \/-(m) => m.map(tpl => (tpl._1, AvroMapType(tpl._2):AvroType[Nu[AvroType]]))
                      }
                    case "fixed" => for {
                      namespace <- fieldAsOpt(json.fields)("namespace")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap{
                          case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                          case _ => M.raiseError[AvroNamespace](UnexpectedParsingResult())
                        }
                      }

                      name <- fieldAs(json.fields)("name")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap{
                          case StringLiteral(s) => refine[String, AvroValidName](s)
                          case _ => M.raiseError[AvroName](UnexpectedParsingResult())
                        }
                      }

                      doc <- fieldAsOpt(json.fields)("doc")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap{
                          case StringLiteral(s) => M.pure(s)
                          case _ => M.raiseError[String](UnexpectedParsingResult())
                        }
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases")(AliasesDefinition()) {
                        case -\/(m) => m.flatMap {
                          case Aliases(aliases) => M.pure(aliases)
                          case _ => M.raiseError[Set[AvroFQN] Refined NonEmpty](UnexpectedParsingResult())
                        }
                      }

                      length <- fieldAs(json.fields)("length")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap{
                          case IntLiteral(i) => refine[Int, Positive](i)
                          case _ => M.raiseError[Int Refined Positive](UnexpectedParsingResult())
                        }
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType = AvroFixedType[Nu[AvroType]](namespace, name, doc, aliases, length):AvroType[Nu[AvroType]]

                    } yield ( Map(fqn.value -> typeBirec.embed(avroType)), avroType)
                    case "enum" => for {
                      namespace <- fieldAsOpt(json.fields)("namespace")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap {
                          case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                          case _ => M.raiseError[AvroNamespace](UnexpectedParsingResult())
                        }
                      }

                      name <- fieldAs(json.fields)("name")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap {
                          case StringLiteral(s) => refine[String, AvroValidName](s)
                          case _ => M.raiseError[AvroName](UnexpectedParsingResult())
                        }
                      }

                      doc <- fieldAsOpt(json.fields)("doc")(LiteralDefinition()) {
                        case -\/(m) => m.flatMap{
                          case StringLiteral(s) => M.pure(s)
                          case _ => M.raiseError[String](UnexpectedParsingResult())
                        }
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases")(AliasesDefinition()) {
                        case -\/(m) => m.flatMap {
                          case Aliases(aliases) => M.pure(aliases)
                          case _ => M.raiseError[Set[AvroFQN] Refined NonEmpty](UnexpectedParsingResult())
                        }
                      }

                      symbols <- fieldAs(json.fields)("symbols")(EnumSymbolDefinition()) {
                        case -\/(m) => m.flatMap {
                          case EnumSymbols(symbols) => M.pure(symbols)
                          case _ => M.raiseError[ListSet[AvroName] Refined NonEmpty](UnexpectedParsingResult())
                        }
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType =AvroEnumType[Nu[AvroType]](namespace, name, doc, aliases, symbols):AvroType[Nu[AvroType]]

                    } yield (Map(fqn.value -> typeBirec.embed(avroType)), avroType)

                    case "record" => {
                      for {
                        namespace <- fieldAsOpt(json.fields)("namespace")(LiteralDefinition()) {
                          case -\/(m) => m.flatMap {
                            case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                            case _ => M.raiseError[AvroNamespace](UnexpectedParsingResult())
                          }
                        }

                        name <- fieldAs(json.fields)("name")(LiteralDefinition()) {
                          case -\/(m) => m.flatMap {
                            case StringLiteral(s) => refine[String, AvroValidName](s)
                            case _ => M.raiseError[AvroName](UnexpectedParsingResult())
                          }
                        }

                        doc <- fieldAsOpt(json.fields)("doc")(LiteralDefinition()) {
                          case -\/(m) => m.flatMap{
                            case StringLiteral(s) => M.pure(s)
                            case _ => M.raiseError[String](UnexpectedParsingResult())
                          }
                        }

                        aliases <- fieldAsOpt(json.fields)("aliases")(AliasesDefinition()) {
                          case -\/(m) => m.flatMap {
                            case Aliases(aliases) => M.pure(aliases)
                            case _ => M.raiseError[Set[AvroFQN] Refined NonEmpty](UnexpectedParsingResult())
                          }
                        }

                        fqn = Util.constructFQN(namespace, name).value

                        fieldsCollection <- fieldAs(json.fields)("fields")(
                          RecordFieldListDefinition(parents + fqn, refs) //this should be the record type created in the yield
                        ) {
                          case -\/(m) => m.flatMap{
                            case FieldDefinitions(fields) => M.pure(fields)
                            case _ => M.raiseError[List[FieldDefinition]](UnexpectedParsingResult())
                          }
                        }

                        fields = fieldsCollection.map(x => x.meta -> x.avroType).toListMap()

                        discovered = fieldsCollection.foldLeft(Map.empty[String, Nu[AvroType]])(
                          (map, elem) => map ++ elem.discoveredTypes
                        )

                        avroType = AvroRecordType(namespace, name, doc, aliases, fields):AvroType[Nu[AvroType]]

                      } yield (discovered + (fqn -> typeBirec.embed(avroType)), avroType)
                    }
                  }
                } yield (discoveryTypeTuple._1, typeBirec.embed(discoveryTypeTuple._2))

                \/-(avroType)
            }
            case RecordFieldDefinition(parents, refs) => {
              val fldDef = for {
                name <- fieldAs(json.fields)("name")(LiteralDefinition()) {
                  case -\/(m) => m.flatMap {
                    case StringLiteral(s) => refine[String, AvroValidName](s)
                    case _ => M.raiseError[AvroName](UnexpectedParsingResult())
                  }
                }

                doc <- fieldAsOpt(json.fields)("doc")(LiteralDefinition()) {
                  case -\/(m) => m.flatMap{
                    case StringLiteral(s) => M.pure(s)
                    case _ => M.raiseError[String](UnexpectedParsingResult())
                  }
                }

                aliases <- fieldAsOpt(json.fields)("aliases")(AliasesDefinition()) {
                  case -\/(m) => m.flatMap {
                    case Aliases(aliases) => M.pure(aliases)
                    case _ => M.raiseError[Set[AvroFQN] Refined NonEmpty](UnexpectedParsingResult())
                  }
                }

                orderOpt <- fieldAsOpt(json.fields)("order")(LiteralDefinition()) {
                  case -\/(m) => m.flatMap{
                    case StringLiteral(s) => s match {
                      case "ignore" => M.pure[AvroRecordSortOrder](ARSOIgnore)
                      case "ascending" => M.pure[AvroRecordSortOrder](ARSOAscending)
                      case "descending" => M.pure[AvroRecordSortOrder](ARSODescending)
                      case _ => M.raiseError[AvroRecordSortOrder](UnknownSortOrder(s))
                    }
                    case _ => M.raiseError[AvroRecordSortOrder](UnexpectedParsingResult())
                  }
                }

                order <- M.pure(orderOpt.getOrElse(ARSOIgnore))

                discoveryTypeTuple <- fieldAs(json.fields)("type")(TypePosition(parents, refs)) {
                  case \/-(m) => m
                  case _ => M.raiseError[(Map[String, Nu[AvroType]], Nu[AvroType])](UnexpectedParsingResult())
                }
              } yield FieldDefinition(discoveryTypeTuple._1, AvroRecordFieldMetaData(name, doc, None, order, aliases), discoveryTypeTuple._2):IntermediateResults

              -\/(fldDef)
            }
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
    }
  }


  private[this] def selectUnionMemberByName[M[_]](members:List[Nu[AvroType]], selector:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    M: MonadError[M, Throwable]
  ):M[AvroType[Nu[AvroType]]] =
    members.map(x => typeBirec.project(x)).flatMap {
      case member: AvroBooleanType[_] =>  if(selector == "boolean") List(member) else Nil
      case member: AvroIntType[_] =>if(selector == "int") List(member) else Nil
      case member: AvroLongType[_] => if(selector == "long") List(member) else Nil
      case member: AvroFloatType[_] => if(selector == "float") List(member) else Nil
      case member: AvroDoubleType[_] => if(selector == "double") List(member) else Nil
      case member: AvroBytesType[_] => if(selector == "bytes") List(member) else Nil
      case member: AvroStringType[_] => if(selector == "string") List(member) else Nil
      case member: AvroRecordType[_] => if(selector == Util.constructFQN(member.namespace, member.name).value) List(member) else Nil
      case member: AvroEnumType[_] => if(selector == Util.constructFQN(member.namespace, member.name).value) List(member) else Nil
      case member: AvroArrayType[_] => if(selector == "array") List(member) else Nil //found out by trying against java reference impl
      case member: AvroMapType[_] => if(selector == "map") List(member) else Nil //found out by trying against java reference impl
      case member: AvroFixedType[_] => if(selector == Util.constructFQN(member.namespace, member.name).value) List(member) else Nil
      case _ : AvroUnionType[_] => Nil //cannot nest unions
      case _ : AvroNullType[_] => Nil //handled seperatly
    }
    match {
      case winner::Nil => M.pure(winner)
      case Nil => M.raiseError(UnionResolutionError("Unknown Union Branch " + selector))
      case x::xs => M.raiseError(UnionResolutionError("Could not disambiguate" + selector))
    }

  def parseAvroDatumAlgebra[M[_], F[_[_]]](
                                            implicit M:MonadError[M, Throwable],
                                            typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
                                            valueBirec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]
                                          ):Algebra[
                                              JsonF,
                                              AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]]
                                            ] =  {

    case json:JsonFNull[_] => {
      case schema:AvroNullType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroNullValue(schema)))
      case schema:AvroUnionType[Nu[AvroType]] =>
        if (
          schema.members.map(typeBirec.project(_)).exists {
            case _:AvroNullType[_] => true
            case _ => false
          }
        )
          M.pure(
            valueBirec.embed(
              AvroUnionValue[Nu[AvroType], F[AvroValue[Nu[AvroType], ?]]](
                schema,
                valueBirec.embed(AvroNullValue[Nu[AvroType], F[AvroValue[Nu[AvroType], ?]]](AvroNullType[Nu[AvroType]]()))
              )
            )
          )
        else
          M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnionError(schema, "Found null object at Union position, but Union did not contain null"))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFTrue[_] => {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, true)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFFalse[_] => {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, false)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberInt[_] => {
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberInt[_] => {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value))) //FIXME OHOH int maybe too small on jsonF
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberDouble[_] => {
      case schema:AvroFloatType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroFloatValue(schema, json.value.toFloat))) //FIXME OHOH 2 shouldn't be an issue but not pretty
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberDouble[_] => {
      case schema:AvroDoubleType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroDoubleValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFString[_] => {
      case schema:AvroBytesType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBytesValue(schema, decodeBytes(json.value))))
      case schema:AvroStringType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroStringValue(schema, json.value)))
      case schema:AvroFixedType[Nu[AvroType]] => {
        val bytes = decodeBytes(json.value)
        if (bytes.length == schema.length.value)
          M.pure(valueBirec.embed(AvroFixedValue(schema, bytes)))
        else
          M.raiseError[F[AvroValue[Nu[AvroType], ?]]](FixedError(schema, bytes.length))
      }
      case schema:AvroEnumType[Nu[AvroType]] =>
        if (schema.symbols.map(_.value).contains(json.value))
          M.pure(valueBirec.embed(AvroEnumValue(schema, json.value)))
        else
          M.raiseError[F[AvroValue[Nu[AvroType], ?]]](EnumError(schema, json.value))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFArray[AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]]] => {
      case schema:AvroArrayType[Nu[AvroType]] => json.values.traverse(f => f(typeBirec.project(schema.items))).map(elems => valueBirec.embed(AvroArrayValue(schema, elems)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFObject[AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]]] => {
      case schema:AvroMapType[Nu[AvroType]] => json.fields.traverse(f => f(typeBirec.project(schema.values))).map(elems => valueBirec.embed(AvroMapValue(schema, elems)))
      case schema:AvroUnionType[Nu[AvroType]] => for {
          member <-
            json.fields.foldLeft(M.pure(Option.empty[(String, AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]])]))(
              (optM, elem) => optM.flatMap{
                case None => M.pure(Some(elem))
                case Some(_) => M.raiseError[Option[(String, AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]])]](UnionError(schema, "Union representation object must contain exactly 1 field"))
              }
            ).flatMap {
              case None => M.raiseError[(String, AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]])](UnionError(schema, "Union representation object must contain exactly 1 field"))
              case Some(x) => M.pure(x)
            }

          memberType <- selectUnionMemberByName[M](schema.members, member._1)

          value <- member._2(memberType)
          union = AvroUnionValue(schema, value)

        } yield valueBirec.embed(union)
      case schema:AvroRecordType[Nu[AvroType]] => {
        val recordFieldNames = schema.fields.keySet.map(_.name)
        val jsonFieldNames = json.fields.keySet

        val missingRecFields = recordFieldNames -- jsonFieldNames
        val unmappedJsonFields = jsonFieldNames -- recordFieldNames

        for {
          _ <- if (missingRecFields.isEmpty) M.pure(()) else M.raiseError[F[AvroValue[Nu[AvroType], ?]]](RecordError(s"declared Fields $missingRecFields are missing"))
          _ <- if (unmappedJsonFields.isEmpty) M.pure(()) else M.raiseError[F[AvroValue[Nu[AvroType], ?]]](RecordError(s"jsonFields Fields $unmappedJsonFields have no schema mapping"))

          fldsWithSchema <- schema.fields.map(kv => kv._1.name -> (kv._1.name, typeBirec.project(kv._2))).traverse(
            kv => M.fromEither(json.fields.get(kv._1).toRight(RecordError(s"json field ${kv._1} is not declares on schema"))).map(f => (kv._2 ,f))
          )

          fields <- fldsWithSchema.traverse(tpl => tpl._2(tpl._1))

          rec = AvroRecordValue(schema, fields)

        } yield valueBirec.embed(rec)
      }
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }

  }




  /*final case class SchemaReferenceStore private (refs:Map[String, Nu[AvroType]])
  final object SchemaReferenceStore{

    implicit def monoidInstance(implicit  typeBirec:Birecursive.Aux[Nu[AvroType], AvroType]) = new Monoid[SchemaReferenceStore]{
      override def zero: SchemaReferenceStore = new SchemaReferenceStore(
        Map(
          "null" -> typeBirec.embed(AvroNullType()),
          "boolean" -> typeBirec.embed(AvroBooleanType()),
          "int" -> typeBirec.embed(AvroIntType()),
          "long" -> typeBirec.embed(AvroLongType()),
          "float" -> typeBirec.embed(AvroFloatType()),
          "double" -> typeBirec.embed(AvroDoubleType()),
          "bytes" -> typeBirec.embed(AvroBytesType()),
          "string" -> typeBirec.embed(AvroStringType())
        )
      )

      override def append(f1: SchemaReferenceStore, f2: => SchemaReferenceStore): SchemaReferenceStore = new SchemaReferenceStore(f1.refs ++ f2.refs)
    }

    def empty: SchemaReferenceStore = monoidInstance.zero

    //TODO: Probably should make sure nobody overwrite the included schemarefs but that'd be stupid ... right?
    def multi(additionalRefs:Map[String, Nu[AvroType]]):SchemaReferenceStore = new SchemaReferenceStore(empty.refs ++ additionalRefs)

    def single(additionalRef:(String, Nu[AvroType])):SchemaReferenceStore = new SchemaReferenceStore(empty.refs + additionalRef)
  }


  type From = (SchemaReferenceStore, ACursor)


  type Out[M[_], A] = M[AvroType[A]]

  //This probably needs a futu and Free monad so we stop if we return a Recursion step
  def decodeSchemaAlgebra[M[_]](
                                          implicit
                                          typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
                                          M: MonadError[M, Throwable]
                                        ): CoalgebraM[
    M,
    Out[M, ?],
    From
    ] = {
    case (refStore: SchemaReferenceStore, cursor:ACursor) if cursor.focus.exists(_.isString) =>for {
          reference <- M.fromEither(as[String](cursor))
          refTypeOpt = refStore.refs.get(reference).map(typeBirec.project)
          _ = println(refTypeOpt)
          avroType <- M.fromEither(refTypeOpt.toRight(UnkownSchemaReference(reference, refStore.refs.keys.toSet)))
        } yield (refStore, avroType)

    case (refStore: SchemaReferenceStore, cursor:ACursor) if cursor.focus.exists(_.isArray) =>
      StateT[M, SchemaReferenceStore, AvroType[From]](
        schemas => for {
            memberCursors <-  M.fromEither(cursor.values.map( jsons => jsons.map(x => (refStore, x.hcursor)).toList ).toRight(UnexpectedTypeError(DecodingFailure("could not decode member list", cursor.history))))
          } yield (schemas,  AvroUnionType(memberCursors))
      )
    //it's a complex type. inspect "type" field and it has to be record, enum, array, map or fixed
    case (refStore: SchemaReferenceStore, cursor:ACursor) if cursor.focus.exists(_.isObject) =>
      StateT[M, SchemaReferenceStore, AvroType[From]](
      schemas => for {
        complexTypeTag <- M.fromEither(as[String](cursor.downField("type")))
        tpl <- complexTypeTag match {
          case "record" => for {
            nameRaw <- asM[M, String](cursor.downField("name"))
            nameSpaceRaw <- asM[M, String](cursor.downField("namespace"))
            name <- refine[M, String, AvroValidName](nameRaw)
            namespace <- refine[M, String, AvroValidNamespace](nameSpaceRaw)
            aliasesRaw <- asM[M, Option[List[String]]](cursor.downField("aliases"))
            doc <- asM[M, Option[String]](cursor.downField("doc"))
            aliasesFQN <- (Traverse[Option] compose Traverse[List]).traverse(aliasesRaw)(alias => refine[M, String, AvroValidNamespace](alias))
            aliases <- aliasesFQN.traverse(lst => refine[M, Set[AvroFQN], NonEmpty](lst.toSet))
            fieldsJson <- M.fromEither(cursor.downField("fields").values.map(_.toList).toRight(UnexpectedTypeError(DecodingFailure("could not decode fields list", cursor.history))))
            fieldsList <- fieldsJson.map(_.hcursor).traverse(
              fldCursor => for {
                name <- asM[M, String](fldCursor.downField("name"))
                doc <- asM[M, Option[String]](fldCursor.downField("doc"))
                defaultString <- asM[M, Option[String]](fldCursor.downField("default"))
                orderString <- asM[M, Option[String]](fldCursor.downField("order"))
                orderOpt <- orderString.traverse {
                  case "ascending" => M.pure[AvroRecordSortOrder](ARSOAscending)
                  case "descending" => M.pure[AvroRecordSortOrder](ARSODescending)
                  case "ignore" => M.pure[AvroRecordSortOrder](ARSOIgnore)
                  case s:String => M.raiseError[AvroRecordSortOrder](UnknownSortOrder(s))
                }
                order = orderOpt.getOrElse(ARSOAscending)
                aliasesFQN <- (Traverse[Option] compose Traverse[List]).traverse(aliasesRaw)(alias => refine[M, String, AvroValidName](alias))
                aliases <- aliasesFQN.traverse(lst => refine[M, Set[AvroName], NonEmpty](lst.toSet))
                typeCursor = fldCursor.downField("type")
              } yield AvroRecordFieldMetaData(name, doc, defaultString, order, aliases) -> typeCursor
            )


            fqn = nameSpaceRaw + "." + nameRaw
            newrefStore = refStore + fqn

            fields = fieldsList.foldLeft(ListMap.empty[AvroRecordFieldMetaData, Free[AvroType, From]])(
              (map, tpl) => map + (tpl._1 -> Free.pure[AvroType, From](newrefStore, tpl._2))
            )
            avroType = AvroRecordType(namespace, name, doc, aliases, fields)

            newSchemas = schemas |+| AvroJsonFAlgebras.SchemaReferenceStore.single(fqn -> avroType)

          } yield (newSchemas,  avroType)
          case "enum" => for {
            nameRaw <- asM[M, String](cursor.downField("name"))
            nameSpaceRaw <- asM[M, String](cursor.downField("namespace"))
            name <- refine[M, String, AvroValidName](nameRaw)
            namespace <- refine[M, String, AvroValidNamespace](nameSpaceRaw)
            aliasesRaw <- asM[M, Option[List[String]]](cursor.downField("aliases"))
            doc <- asM[M, Option[String]](cursor.downField("doc"))
            aliasesFQN <- (Traverse[Option] compose Traverse[List]).traverse(aliasesRaw)(alias => refine[M, String, AvroValidNamespace](alias))
            aliases <- aliasesFQN.traverse(lst => refine[M, Set[AvroFQN], NonEmpty](lst.toSet))
            symbolsRaw <- asM[M, List[String]](cursor.downField("symbols"))
            symbolsList <- symbolsRaw.traverse( symbol => refine[M, String, AvroValidName](symbol))
            symbols = symbolsList.foldLeft(ListSet.empty[AvroName])((set, symbol) => set + symbol)
            avroType = AvroEnumType[Free[AvroType, From]](namespace, name, doc, aliases, symbols)

            fqn = nameSpaceRaw + "." + nameRaw
            newSchemas = schemas |+| AvroJsonFAlgebras.SchemaReferenceStore.single(fqn -> avroType)
            newrefStore:SchemaReferenceStore = refStore + fqn
          } yield (newSchemas, avroType)

          case "array" => M.pure((schemas, AvroArrayType(Free.pure[AvroType, From](refStore, cursor.downField("items")))))

          case "map" => M.pure((schemas, AvroMapType(Free.pure[AvroType, From](refStore, cursor.downField("values")))))

          case "fixed" => for {
            nameRaw <- asM[M, String](cursor.downField("name"))
            nameSpaceRaw <- asM[M, String](cursor.downField("namespace"))
            name <- refine[M, String, AvroValidName](nameRaw)
            namespace <- refine[M, String, AvroValidNamespace](nameSpaceRaw)
            aliasesRaw <- asM[M, Option[List[String]]](cursor.downField("aliases"))
            doc <- asM[M, Option[String]](cursor.downField("doc"))
            lengthRaw <- asM[M, Int](cursor.downField("length"))
            length <- refine[M, Int, Positive](lengthRaw)
            aliasesFQN <- (Traverse[Option] compose Traverse[List]).traverse(aliasesRaw)(alias => refine[M, String, AvroValidNamespace](alias))
            aliases <- aliasesFQN.traverse(lst => refine[M, Set[AvroFQN], NonEmpty](lst.toSet))
            avroType = AvroFixedType[Free[AvroType, From]](namespace, name, doc, aliases, length)
            fqn = nameSpaceRaw + "." + nameRaw
            newSchemas = schemas |+| AvroJsonFAlgebras.SchemaReferenceStore.single(fqn -> avroType)
            newrefStore = refStore + fqn
          } yield (newSchemas, avroType)

          case x:String => M.raiseError[(SchemaReferenceStore, AvroType[From])](UnallowedTypeIdentifier(x))
        }
      } yield tpl
    )

  }*/



  /*def decodeDatumAlgebra[M[_], F[_[_]]](
      implicit
      birec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
      typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
      M: MonadError[M, Throwable]
    ): CoalgebraM[
      M,
      AvroValue[Nu[AvroType], ?],
      (ACursor, Nu[AvroType])
    ] =
    tpl => (tpl._1, typeBirec.project(tpl._2)) match {
      case (cursor:ACursor, schema:AvroNullType[Nu[AvroType]]) => M.fromEither(as[Null](cursor)).map( _ => AvroNullValue(schema))
      case (cursor:ACursor, schema:AvroBooleanType[Nu[AvroType]]) =>  M.fromEither(as[Boolean](cursor)).map( x => AvroBooleanValue(schema, x))
      case (cursor:ACursor, schema:AvroIntType[Nu[AvroType]]) =>  M.fromEither(as[Int](cursor)).map( x => AvroIntValue(schema, x))
      case (cursor:ACursor, schema:AvroLongType[Nu[AvroType]]) =>  M.fromEither(as[Long](cursor)).map( x => AvroLongValue(schema, x))
      case (cursor:ACursor, schema:AvroFloatType[Nu[AvroType]]) =>  M.fromEither(as[Float](cursor)).map( x => AvroFloatValue(schema, x))
      case (cursor:ACursor, schema:AvroDoubleType[Nu[AvroType]]) =>  M.fromEither(as[Double](cursor)).map( x => AvroDoubleValue(schema, x))
      case (cursor:ACursor, schema:AvroBytesType[Nu[AvroType]]) =>  M.fromEither(as[String](cursor)).map( x => AvroBytesValue(schema, decodeBytes(x)))
      case (cursor:ACursor, schema:AvroStringType[Nu[AvroType]]) =>  M.fromEither(as[String](cursor)).map( x => AvroStringValue(schema, x))
      case (cursor:ACursor, schema:AvroFixedType[Nu[AvroType]]) =>  M.fromEither(as[String](cursor)).map( x => AvroFixedValue(schema, decodeBytes(x)))
      case (cursor:ACursor, schema:AvroEnumType[Nu[AvroType]]) =>  for {
        possibleSymbol <- M.fromEither(as[String](cursor))
        rawSymbols = schema.symbols.map(_.value)
        _ <- if(!rawSymbols.contains(possibleSymbol)) M.raiseError(EnumError(rawSymbols, possibleSymbol)) else M.pure(())
      } yield AvroEnumValue(schema, possibleSymbol)

      case (cursor:ACursor, schema:AvroUnionType[Nu[AvroType]]) => {
        if (as[Null](cursor).isRight && schema.members.map(t => typeBirec.project(t)).collect { case _:AvroNullType[Nu[AvroType]]=> ()}.nonEmpty) M.pure(AvroNullValue(AvroNullType()))
        else if (cursor.keys.isDefined) for {
          keys <- M.fromEither(cursor.keys.toRight(UnionError("was not an object"))).map(_.toList)
          key <- if (keys.length != 1) M.raiseError(UnionError("Union Object must have EXACTLY one property")) else M.pure(keys.head)
          member <- selectUnionMemberByName[M, F](schema.members, key, cursor)
        } yield AvroUnionValue(schema, member)
        else M.raiseError(UnionError(cursor.focus.map(_.noSpaces).getOrElse("Broken Cursor") + " is not a valid union representation can only be null or an object with a single property"))
      }

      case (cursor:ACursor, schema:AvroArrayType[Nu[AvroType]]) => for {
          arrayEntries <- M.fromEither(cursor.values.toRight(UnionError("was not an object")))
          items = arrayEntries.map(json => (json.hcursor, schema.items)).toList
        } yield AvroArrayValue(schema, items)


      case (cursor:ACursor, schema:AvroMapType[Nu[AvroType]]) => for {
        keys <- M.fromEither(cursor.keys.toRight(UnionError("was not an object")))
        map = keys.map(k => k -> (cursor.downField(k), schema.values)).toMap
      } yield AvroMapValue(schema, map)

      case (cursor:ACursor, schema:AvroRecordType[Nu[AvroType]]) => for {
        keysOnObj <- M.fromEither(cursor.keys.toRight(UnionError("was not an object")))
        keysOnSchema = schema.fields.keys.map(_.name)
        missing = keysOnSchema.toSet -- keysOnObj.toSet
        superflous =  keysOnObj.toSet -- keysOnSchema.toSet
        _ <- if (missing.nonEmpty) M.raiseError(RecordError("Missing properties on JSON " + missing)) else M.pure(())
        _ <- if (superflous.nonEmpty) M.raiseError(RecordError("Too many properties on record " + superflous)) else M.pure(())
        map = schema.fields.foldLeft(ListMap.empty[String, (ACursor, Nu[AvroType])])((map, fld) =>  map + (fld._1.name -> (cursor.downField(fld._1.name), fld._2)))
      } yield AvroRecordValue(schema, map)

    }



  def decode[M[_], F[_[_]]](schema:Nu[AvroType], jsonMessage:String)(
                             implicit
                             birec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
                             typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
                             M: MonadError[M, Throwable]
                           ):M[F[AvroValue[Nu[AvroType], ?]]] = for {
    cursor <- M.fromEither(parse(jsonMessage)).map(_.hcursor)
    tpl:(ACursor, Nu[AvroType]) = (cursor, schema)
    result <- tpl.anaM.apply(decodeDatumAlgebra[M, F])
  } yield result*/


  def parseDatum[M[_], F[_[_]]](schema:Nu[AvroType])(avroJsonRepr:String)(
    implicit M:MonadError[M, Throwable],
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    valueBirec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]
  ):M[F[AvroValue[Nu[AvroType], ?]]] = for {
    jsonF <- parseJsonF[M,Fix](avroJsonRepr)
    resultFunction = jsonF.cata(parseAvroDatumAlgebra[M, F])
    result <- resultFunction(typeBirec.project(schema))
  } yield result

  def parseSchema[M[_]](schemaString:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    M: MonadError[M, Throwable]
  ):M[Nu[AvroType]] = for {
    jsonF <- parseJsonF[M,Fix](schemaString)
    resultFunction = jsonF.cata(parseAvroSchemaAlgebra[M, Fix])
    result <- M.pure(resultFunction(
      TypePosition(
        Set.empty[String],
        Map(
        "null" -> typeBirec.embed(AvroNullType()),
        "boolean" -> typeBirec.embed(AvroBooleanType()),
        "int" -> typeBirec.embed(AvroIntType()),
        "long" -> typeBirec.embed(AvroLongType()),
        "float" -> typeBirec.embed(AvroFloatType()),
        "double" -> typeBirec.embed(AvroDoubleType()),
        "bytes" -> typeBirec.embed(AvroBytesType()),
        "string" -> typeBirec.embed(AvroStringType())
        )
      )
    ))

    out <- result match {
      case -\/(_) => M.raiseError[Nu[AvroType]](new RuntimeException("Invalid Parser State"))
      case \/-(x) => x.map(_._2)
    }

  } yield out


}
