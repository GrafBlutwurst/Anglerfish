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
import matryoshka.{Algebra, Birecursive, Coalgebra}
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
                if (parents.contains(json.value))
                  M.pure(
                    (
                      Map.empty[String, Nu[AvroType]],
                      typeBirec.embed(
                        AvroRecursionType[Nu[AvroType]](
                          json.value,
                          typeBirec.embed(AvroNullType[Nu[AvroType]]())
                        ))
                    )
                  ) //FIXME: is this really the only way?!?
                else
                  refs.get(json.value) match {
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

                        avroTypePass1 = typeBirec.embed(AvroRecordType(namespace, name, doc, aliases, fields)):Nu[AvroType]


                        unfoldCoalg:Coalgebra[AvroType, Nu[AvroType]] =  (nu:Nu[AvroType]) => {
                          typeBirec.project(nu) match {
                            case art:AvroRecursionType[Nu[AvroType]] if art.fqn == fqn =>
                              AvroRecursionType[Nu[AvroType]](
                                fqn,
                                avroTypePass1
                              )
                            case x:AvroType[Nu[AvroType]] => x
                          }
                        }



                        avroTypeNu:Nu[AvroType] = avroTypePass1.cata(
                          (avroType:AvroType[Nu[AvroType]]) => avroType match {
                            case art:AvroRecursionType[Nu[AvroType]] if art.fqn == fqn => typeBirec.embed(AvroRecursionType[Nu[AvroType]](fqn, Nu(unfoldCoalg, avroTypePass1)))
                            case x: AvroType[Nu[AvroType]] => typeBirec.embed(x)
                          }
                        )

                        avroType = typeBirec.project(avroTypeNu)

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

  private def expandRecursiveReference(schema:AvroType[Nu[AvroType]])(implicit typeBirec:Birecursive.Aux[Nu[AvroType], AvroType]):AvroType[Nu[AvroType]] = schema match {
    case member: AvroRecursionType[Nu[AvroType]] => typeBirec.project(member.lazyType)
    case x: AvroType[Nu[AvroType]] => x
  }


  private[this] def selectUnionMemberByName[M[_]](members:List[Nu[AvroType]], selector:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    M: MonadError[M, Throwable]
  ):M[AvroType[Nu[AvroType]]] =
    members
      .map(x => typeBirec.project(x))
      .map(expandRecursiveReference)
      .flatMap {
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
      case _ : AvroRecursionType[_] => Nil //impossible to reach due to previous map but else the compiler is crying
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
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value))) //FIXME OHOH int maybe too small on jsonF
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberDouble[_] => {
      case schema:AvroFloatType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroFloatValue(schema, json.value.toFloat))) //FIXME OHOH 2 shouldn't be an issue but not pretty
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
      case schema:AvroArrayType[Nu[AvroType]] => json.values.traverse(f => f(expandRecursiveReference(typeBirec.project(schema.items)))).map(elems => valueBirec.embed(AvroArrayValue(schema, elems)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFObject[AvroType[Nu[AvroType]] => M[F[AvroValue[Nu[AvroType], ?]]]] => {
      case schema:AvroMapType[Nu[AvroType]] => json.fields.traverse(f => f(expandRecursiveReference(typeBirec.project(schema.values)))).map(elems => valueBirec.embed(AvroMapValue(schema, elems)))
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

          fldsWithSchema <- schema.fields.map(kv => kv._1.name -> (kv._1.name, expandRecursiveReference(typeBirec.project(kv._2)))).traverse(
            kv => M.fromEither(json.fields.get(kv._1).toRight(RecordError(s"json field ${kv._1} is not declares on schema"))).map(f => (kv._2 ,f))
          )

          fields <- fldsWithSchema.traverse(tpl => tpl._2(tpl._1))

          rec = AvroRecordValue(schema, fields)

        } yield valueBirec.embed(rec)
      }
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[F[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }

  }





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
