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
  final case class InvalidParserState[T](parserContext:AvroSchemaParsingContext, expected:String, got:T) extends AvroSchemaErrors
  final case class UnknownFieldError(field:String, available:Set[String]) extends AvroSchemaErrors
  final case class RefinementError(error:String) extends AvroSchemaErrors
  final case class UnknownSortOrder(error:String) extends AvroSchemaErrors



  private[this] def decodeBytes(s:String):Vector[Byte] = Base64.getDecoder.decode(s).toVector

  sealed trait AvroSchemaParsingContext
  final case class TypePosition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class RecordFieldDefinition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class RecordFieldListDefinition(parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends AvroSchemaParsingContext
  final case class DefaultDefinition() extends AvroSchemaParsingContext
  final case class LiteralDefinition() extends AvroSchemaParsingContext
  final case class AliasesDefinition() extends AvroSchemaParsingContext
  final case class DefaultValueDefition() extends AvroSchemaParsingContext
  final case class EnumSymbolDefinition() extends AvroSchemaParsingContext



  sealed trait IntermediateResults
  final case class NullLiteral() extends IntermediateResults
  final case class BooleanLiteral(b:Boolean) extends IntermediateResults
  final case class IntLiteral(i:Long) extends IntermediateResults
  final case class DoubleLiteral(d:Double) extends IntermediateResults
  final case class StringLiteral(s:String) extends IntermediateResults
  final case class JsonFLiteral[F[_[_]]](jsonF:F[JsonF]) extends IntermediateResults
  final case class Aliases(aliases:Set[AvroFQN] Refined NonEmpty) extends IntermediateResults
  final case class EnumSymbols(symbols:ListSet[AvroName] Refined NonEmpty) extends IntermediateResults
  final case class FieldDefinition(discoveredTypes: Map[String, Nu[AvroType]], meta:AvroRecordFieldMetaData, avroType: Nu[AvroType]) extends IntermediateResults
  final case class FieldDefinitions(fields:List[FieldDefinition]) extends IntermediateResults


  /*
  Some explanation is waranted here. So IF we already had dependent functiontypes this algebra would look something like this
  Algebra[JsonF, AvroParsingContext => AvroParsingContext.Out]

  So this is what's happening here. Due to the nature of schemas (being possibly infinite) you can't quite use a CoAlgebraM because what will happen is Nu[M[A]] => M[Nu[A]] which forces
  the entire Nu, thus probably running infinitely. Greg Pfeil pointed this out together with the solution https://github.com/sellout/recursion-schemes-cookbook/tree/drafts/AttributeGrammars
  rather then outputing the result directly in the fold, we output a function from some context to our result type. This allows us to effectively pass parameters INTO the fold.

  Now about the either. We unfold a JsonF into an AvroType which is substentially richer than JsonF. This means we do NOT have 1 to 1 mappings from the members of the JsonF GADT to the AvroType one.
  Now this means when we traverse through JsonF we have to be aware what we're looking at e.g. a JsonObject has to be a type if it is at the root (type position) or if it's in a property called "type" but
  on the other hand if it is in a List which is in turn in a property called "fields" it means the object is actually a record field definition.

  This context is expressed in the AvroSchemaParsingContext ADT. Not every of those context will result in a AvroType. which is where the IntermediateResults come into play.
  So a given step can either return an IntermediateResult or an AvroType. Because this can also have errors it needs to be in some M. This gets us to the below signature
  now ideally we would have dependent function types, allowing us to express this because in the parseSchema function we call a cata with this algebra and pass the initial state which is
  TypePosition. The dependent type of TypePosition would be AvroType
   */

  //FIXME error handling
  def parseAvroSchemaAlgebra[M[_], F[_[_]]](implicit M:MonadError[M, Throwable], jsonBirec:Birecursive.Aux[F[JsonF], JsonF], typeBirec:Birecursive.Aux[Nu[AvroType], AvroType], valueBirec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]):
    Algebra[
      JsonF,
      AvroSchemaParsingContext => M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])]
    ] = jsonF => {

        type Out = AvroSchemaParsingContext => M[IntermediateResults] \/ M[(Map[String, Nu[AvroType]], Nu[AvroType])]


        def refine[A, P](a: A)(implicit validate:Validate[A, P]) = M.fromEither(refineV[P](a).left.map(RefinementError))

        def fieldAsOpt[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(eitherF: 
            PartialFunction[IntermediateResults, M[T]] \/
            ((Map[String, Nu[AvroType]], Nu[AvroType]) => M[T])
        ):M[Option[T]] =
          fields
            .get(field)
            .map(eval => eval(parsingContext))
            .traverse(
              out => (out, eitherF) match {
                case (-\/(m), -\/(f)) => m.flatMap( x => if (f.isDefinedAt(x)) f(x) else  M.raiseError[T](InvalidParserState(parsingContext, expected, x))) 
                case (\/-(m), \/-(f)) => m.flatMap( x => f(x._1, x._2) )
                case (-\/(m), _) => m.flatMap(e => M.raiseError[T](InvalidParserState(parsingContext, expected, e)))
                case (\/-(m), _) => m.flatMap(e => M.raiseError[T](InvalidParserState(parsingContext, expected, e)))
              }
            )

        def fieldAs[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(
          eitherF: 
            PartialFunction[IntermediateResults, M[T]] \/
            ((Map[String, Nu[AvroType]], Nu[AvroType]) => M[T])
        
        ):M[T] =
          fieldAsOpt[T](fields)(field, expected)(parsingContext)(eitherF).flatMap {
            case Some(x) => M.pure(x)
            case None => M.raiseError[T](UnknownFieldError(field, fields.keySet))
          }

        def fieldAsL[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(
          f: 
            PartialFunction[IntermediateResults, M[T]]
        ):M[T] = fieldAs[T](fields)(field, expected)(parsingContext)(-\/(f))


        def fieldAsR[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(
          f: 
            (Map[String, Nu[AvroType]], Nu[AvroType]) =>  M[T]
        ):M[T] = fieldAs[T](fields)(field, expected)(parsingContext)(\/-(f))


        def fieldAsLOpt[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(
          f: 
            PartialFunction[IntermediateResults, M[T]]
        ):M[Option[T]] = fieldAsOpt[T](fields)(field, expected)(parsingContext)(-\/(f))


        def fieldAsROpt[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: AvroSchemaParsingContext
        )(
          f: 
              (Map[String, Nu[AvroType]], Nu[AvroType]) =>  M[T]
        ):M[Option[T]] = fieldAsOpt[T](fields)(field, expected)(parsingContext)(\/-(f))


        jsonF match {
          case json:JsonFNull[Out]         =>  {
            case LiteralDefinition() => -\/(M.pure(NullLiteral()))
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFNull[F[JsonF]]()))))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFTrue[Out]         =>  {
            case LiteralDefinition() => -\/(M.pure(BooleanLiteral(true)))
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFTrue[F[JsonF]]()))))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFFalse[Out]        =>  {
            case LiteralDefinition() => -\/(M.pure(BooleanLiteral(false)))
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFFalse[F[JsonF]]()))))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFNumberDouble[Out] =>  {
            case LiteralDefinition() => -\/(M.pure(DoubleLiteral(json.value)))
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberDouble[F[JsonF]](json.value)))))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFNumberInt[Out]    =>  {
            case LiteralDefinition() => -\/(M.pure(IntLiteral(json.value)))
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberInt[F[JsonF]](json.value)))))
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
            case DefaultDefinition() => -\/(M.pure(JsonFLiteral(jsonBirec.embed(JsonFString[F[JsonF]](json.value)))))
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          // Can be a Union if in type position, or a list of fields if in the fields property
          case json:JsonFArray[Out] => {
            case context@TypePosition(parents, refs) => {
              val evaluated = json.values
                .map(f => f(TypePosition(parents, refs)))
                .traverse {
                  case -\/(em) => em.flatMap( e => M.raiseError[(Map[String, Nu[AvroType]], Nu[AvroType])](InvalidParserState(context, "AvroType", e)))
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
            case context@RecordFieldListDefinition(parents, refs) => -\/(
              json.values.foldLeft(
                M.pure((Map.empty[String, Nu[AvroType]], List.empty[FieldDefinition]))
              )(
                (mOuter, f) => mOuter.flatMap(
                  tpl => {
                    f(RecordFieldDefinition(parents, refs ++ tpl._1)) match {
                      case -\/(m) => m.flatMap{
                        case f: FieldDefinition => M.pure((tpl._1 ++ f.discoveredTypes, tpl._2 ::: List(f)))
                        case e => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](InvalidParserState(context, "field definition",  e))
                      }
                      case \/-(me) => me.flatMap( e => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](InvalidParserState(context, "field definition",  e)))
                    }
                  }
                )

              ).map(
                tpl => FieldDefinitions(tpl._2)
              )
            )
            case DefaultDefinition() => -\/(
              json.values.traverse(
                f => f(DefaultDefinition()).fold(
                  lm => lm.flatMap {
                    case jl:JsonFLiteral[F] => M.pure(jl.jsonF)
                    case x => M.raiseError[F[JsonF]](InvalidParserState(DefaultDefinition(), "default jsonF of array", x))
                  },
                  rm => rm.flatMap(t => M.raiseError[F[JsonF]](InvalidParserState(DefaultDefinition(), "default jsonF of array", t)))
                )
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFArray(values))))
            )
            case context: AvroSchemaParsingContext => \/-(M.raiseError(UnexpectedJsonType(json, context)))
          }
          case json:JsonFObject[Out] => {
            case context@TypePosition(parents, refs) => {
                val avroType = for {
                  typeString <- fieldAsL(json.fields)("type", "avro complex type String literal")(LiteralDefinition()){
                        case StringLiteral(s) => M.pure(s)
                      }
                  

                  discoveryTypeTuple <- typeString match {
                    case "array" =>
                      fieldAsR(json.fields)("items", "array items type definition")(TypePosition(parents, refs))(
                        (refs, avroType) => M.pure(refs, AvroArrayType(avroType):AvroType[Nu[AvroType]])
                      )
                    case "map" =>
                      fieldAsR(json.fields)("values", "map value type definition")(TypePosition(parents, refs))(
                        (refs, avroType) => M.pure(refs, AvroMapType(avroType):AvroType[Nu[AvroType]])
                      )
                    case "fixed" => for {
                      namespace <- fieldAsLOpt(json.fields)("namespace", "fixed namespace String literal")(LiteralDefinition()){
                        case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                      }
                     
                      name <- fieldAsL(json.fields)("name", "fixed name String literal")(LiteralDefinition()) {
                        case StringLiteral(s) => refine[String, AvroValidName](s)
                      }

                      doc <- fieldAsLOpt(json.fields)("doc", "fixed doc String literal")(LiteralDefinition()) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsLOpt(json.fields)("aliases", "fixed aliases")(AliasesDefinition()) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      length <- fieldAsL(json.fields)("length", "fixed length")(LiteralDefinition()) {
                        case IntLiteral(i) => refine[Int, Positive](i.toInt)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType = AvroFixedType[Nu[AvroType]](namespace, name, doc, aliases, length):AvroType[Nu[AvroType]]

                    } yield ( Map(fqn.value -> typeBirec.embed(avroType)), avroType)
                    case "enum" => for {
                      namespace <- fieldAsLOpt(json.fields)("namespace", "enum namespace")(LiteralDefinition()) {
                        case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                      }

                      name <- fieldAsL(json.fields)("name", "enum name")(LiteralDefinition()) {
                        case StringLiteral(s) => refine[String, AvroValidName](s)
                      }

                      doc <- fieldAsLOpt(json.fields)("doc", "enum doc")(LiteralDefinition()) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsLOpt(json.fields)("aliases", "enum aliases")(AliasesDefinition()) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      symbols <- fieldAsL(json.fields)("symbols", "enum symbols")(EnumSymbolDefinition()) {
                        case EnumSymbols(symbols) => M.pure(symbols)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType =AvroEnumType[Nu[AvroType]](namespace, name, doc, aliases, symbols):AvroType[Nu[AvroType]]

                    } yield (Map(fqn.value -> typeBirec.embed(avroType)), avroType)

                    case "record" => {



                      for {
                        namespace <- fieldAsLOpt(json.fields)("namespace", "record namespace")(LiteralDefinition()) {
                          case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                        }

                        name <- fieldAsL(json.fields)("name", "record name")(LiteralDefinition()) {
                          case StringLiteral(s) => refine[String, AvroValidName](s)
                        }

                        doc <- fieldAsLOpt(json.fields)("doc", "record doc")(LiteralDefinition()) {
                          case StringLiteral(s) => M.pure(s)
                        }

                        aliases <- fieldAsLOpt(json.fields)("aliases", "record aliases")(AliasesDefinition()) {
                          case Aliases(aliases) => M.pure(aliases)
                        }

                        fqn = Util.constructFQN(namespace, name).value

                        fieldsCollection <- fieldAsL(json.fields)("fields", "record fields")(
                          RecordFieldListDefinition(parents + fqn, refs) 
                        ) {
                          case FieldDefinitions(fields) => M.pure(fields)
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
                name <- fieldAsL(json.fields)("name", "field name")(LiteralDefinition()) {
                  case StringLiteral(s) => refine[String, AvroValidName](s)
                }

                doc <- fieldAsLOpt(json.fields)("doc", "field doc")(LiteralDefinition()) {
                  case StringLiteral(s) => M.pure(s)
                }

                aliases <- fieldAsLOpt(json.fields)("aliases", "field aliases")(AliasesDefinition()) {
                  case Aliases(aliases) => M.pure(aliases)
                }

                orderOpt <- fieldAsLOpt(json.fields)("order", "field order")(LiteralDefinition()) {
                    case StringLiteral(s) => s match {
                      case "ignore" => M.pure[AvroRecordSortOrder](ARSOIgnore)
                      case "ascending" => M.pure[AvroRecordSortOrder](ARSOAscending)
                      case "descending" => M.pure[AvroRecordSortOrder](ARSODescending)
                      case _ => M.raiseError[AvroRecordSortOrder](UnknownSortOrder(s))
                    }
                }

                order <- M.pure(orderOpt.getOrElse(ARSOIgnore))

                discoveryTypeTuple <- fieldAsR(json.fields)("type", "field type")(TypePosition(parents, refs))((refs, avroType) => M.pure((refs, avroType)))

                fieldType = discoveryTypeTuple._2

                defaultAvroValue <- fieldAsLOpt(json.fields)("default", "default value")(DefaultDefinition()) {
                  case jl:JsonFLiteral[F] => {
                    val parser = jsonBirec.cata(jl.jsonF)(parseAvroDatumAlgebra[M])
                    parser(typeBirec.project(fieldType))
                  }
                }

              } yield FieldDefinition(discoveryTypeTuple._1, AvroRecordFieldMetaData(name, doc, defaultAvroValue, order, aliases), discoveryTypeTuple._2):IntermediateResults

              -\/(fldDef)
            }

            case DefaultDefinition() => -\/(
              json.fields.traverse(
                f => f(DefaultDefinition()).fold(
                  lm => lm.flatMap {
                    case jl:JsonFLiteral[F] => M.pure(jl.jsonF)
                    case x => M.raiseError[F[JsonF]](InvalidParserState(DefaultDefinition(), "default jsonF of object", x))
                  },
                  rm => rm.flatMap(t => M.raiseError[F[JsonF]](InvalidParserState(DefaultDefinition(), "default jsonF of object", t)))
                )
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFObject(values))))
            )

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

  def parseAvroDatumAlgebra[M[_]](
                                            implicit M:MonadError[M, Throwable],
                                            typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
                                            valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]
                                          ):Algebra[
                                              JsonF,
                                              AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]]
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
              AvroUnionValue[Nu[AvroType], Fix[AvroValue[Nu[AvroType], ?]]](
                schema,
                valueBirec.embed(AvroNullValue[Nu[AvroType], Fix[AvroValue[Nu[AvroType], ?]]](AvroNullType[Nu[AvroType]]()))
              )
            )
          )
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnionError(schema, "Found null object at Union position, but Union did not contain null"))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFTrue[_] => {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, true)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFFalse[_] => {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, false)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberInt[_] => {
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value.toInt))) //FIXME OH GOD NO
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value))) //FIXME OHOH int maybe too small on jsonF
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberDouble[_] => {
      case schema:AvroFloatType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroFloatValue(schema, json.value.toFloat))) //FIXME OHOH 2 shouldn't be an issue but not pretty
      case schema:AvroDoubleType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroDoubleValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFString[_] => {
      case schema:AvroBytesType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBytesValue(schema, decodeBytes(json.value))))
      case schema:AvroStringType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroStringValue(schema, json.value)))
      case schema:AvroFixedType[Nu[AvroType]] => {
        val bytes = decodeBytes(json.value)
        if (bytes.length == schema.length.value)
          M.pure(valueBirec.embed(AvroFixedValue(schema, bytes)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](FixedError(schema, bytes.length))
      }
      case schema:AvroEnumType[Nu[AvroType]] =>
        if (schema.symbols.map(_.value).contains(json.value))
          M.pure(valueBirec.embed(AvroEnumValue(schema, json.value)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](EnumError(schema, json.value))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFArray[AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]]] => {
      case schema:AvroArrayType[Nu[AvroType]] => json.values.traverse(f => f(expandRecursiveReference(typeBirec.project(schema.items)))).map(elems => valueBirec.embed(AvroArrayValue(schema, elems)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFObject[AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]]] => {
      case schema:AvroMapType[Nu[AvroType]] => json.fields.traverse(f => f(expandRecursiveReference(typeBirec.project(schema.values)))).map(elems => valueBirec.embed(AvroMapValue(schema, elems)))
      case schema:AvroUnionType[Nu[AvroType]] => for {
          member <-
            json.fields.foldLeft(M.pure(Option.empty[(String, AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]])]))(
              (optM, elem) => optM.flatMap{
                case None => M.pure(Some(elem))
                case Some(_) => M.raiseError[Option[(String, AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]])]](UnionError(schema, "Union representation object must contain exactly 1 field"))
              }
            ).flatMap {
              case None => M.raiseError[(String, AvroType[Nu[AvroType]] => M[Fix[AvroValue[Nu[AvroType], ?]]])](UnionError(schema, "Union representation object must contain exactly 1 field"))
              case Some(x) => M.pure(x)
            }

          memberType <- selectUnionMemberByName[M](schema.members, member._1)

          value <- member._2(memberType)
          union = AvroUnionValue(schema, value)

        } yield valueBirec.embed(union)
      case schema:AvroRecordType[Nu[AvroType]] => {
        val recordFieldNames = schema.fields.keySet.map(_.name)
        val fieldsWithDefaults = schema.fields.keySet.flatMap(
          definition => if (definition.default.isDefined) Set(definition.name) else Set.empty[String]
        )
        val requiredFieldNames = recordFieldNames -- fieldsWithDefaults
        val jsonFieldNames = json.fields.keySet

        val missingRecFields = requiredFieldNames -- jsonFieldNames
        val unmappedJsonFields = jsonFieldNames -- recordFieldNames

        for {
          _ <- if (missingRecFields.isEmpty) M.pure(()) else M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](RecordError(s"declared Fields $missingRecFields are missing"))
          _ <- if (unmappedJsonFields.isEmpty) M.pure(()) else M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](RecordError(s"jsonFields Fields $unmappedJsonFields have no schema mapping"))
          
          
          fields <- schema.fields.map(kv => kv._1.name -> (kv._1, expandRecursiveReference(typeBirec.project(kv._2)))).traverse(
            kv => {
              M.bind(
                M.fromEither(
                  (json.fields.get(kv._1.name).map(f => f(kv._2)) <+> kv._1.default.map(fix => M.pure(fix))).toRight(RecordError(s"schema field ${kv._1} is not on json and has no default"))
                )
              )(identity)
            }

          )

          rec = AvroRecordValue(schema, fields)

        } yield valueBirec.embed(rec)
      }
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }

  }





  def parseDatum[M[_]](schema:Nu[AvroType])(avroJsonRepr:String)(
    implicit M:MonadError[M, Throwable],
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]
  ):M[Fix[AvroValue[Nu[AvroType], ?]]] = for {
    jsonF <- parseJsonF[M,Fix](avroJsonRepr)
    resultFunction = jsonF.cata(parseAvroDatumAlgebra[M])
    result <- resultFunction(typeBirec.project(schema))
  } yield result

  def parseSchema[M[_]](schemaString:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
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
