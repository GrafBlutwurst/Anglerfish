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
import ch.grafblutwurst.anglerfish.data.avro.AvroJsonFAlgebras.ParsingContext.HistEntry
import matryoshka.{Algebra, Birecursive, Coalgebra, Corecursive, Recursive}
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

import scala.collection.immutable.{ListMap, ListSet, Queue, Stack}

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
  final case class UnrepresentableError(base:JsonF[_]) extends AvroDatumError


  sealed trait AvroSchemaErrors extends AvroError
  final case class UnkownSchemaReference(errorReference:String, knownReferences: Set[String]) extends AvroSchemaErrors
  final case class UnexpectedParsingResult[T](dispatchingContext:ParsingContext, expected:String, got:T) extends AvroSchemaErrors
  final case class InvalidParserState(parserContext:ParsingContext, validStates:Set[String]) extends AvroSchemaErrors
  final case class UnknownFieldError(field:String, available:Set[String]) extends AvroSchemaErrors
  final case class RefinementError(error:String) extends AvroSchemaErrors
  final case class UnknownSortOrder(error:String) extends AvroSchemaErrors



  private[this] def decodeBytes(s:String):Vector[Byte] = Base64.getDecoder.decode(s).toVector

  sealed trait ParsingContext {
    val history: List[HistEntry]
    val encolsingNamespace: List[AvroNamespace]
  }
  object ParsingContext{
    final case class HistEntry(context:ParsingContext, jsonF: JsonF[_])

    def root(implicit typeCorec:Corecursive.Aux[Nu[AvroType], AvroType[?]]) = TypePosition(
      List.empty[HistEntry],
      List.empty[AvroNamespace],
      Set.empty[String],
      Map(
        "null" -> typeCorec.embed(AvroNullType()),
        "boolean" -> typeCorec.embed(AvroBooleanType()),
        "int" -> typeCorec.embed(AvroIntType()),
        "long" -> typeCorec.embed(AvroLongType()),
        "float" -> typeCorec.embed(AvroFloatType()),
        "double" -> typeCorec.embed(AvroDoubleType()),
        "bytes" -> typeCorec.embed(AvroBytesType()),
        "string" -> typeCorec.embed(AvroStringType())
      )
    )
  }
  final case class TypePosition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends ParsingContext
  final case class RecordFieldDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends ParsingContext
  final case class RecordFieldListDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends ParsingContext
  final case class DefaultDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends ParsingContext
  final case class LiteralDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends ParsingContext
  final case class AliasesDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends ParsingContext
  final case class DefaultValueDefition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends ParsingContext
  final case class EnumSymbolDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends ParsingContext



  sealed trait ParsingResult
  final case class NullLiteral() extends ParsingResult
  final case class BooleanLiteral(b:Boolean) extends ParsingResult
  final case class BigIntLiteral(i:BigInt) extends ParsingResult
  final case class LongLiteral(i:Long) extends ParsingResult
  final case class IntLiteral(i:Int) extends ParsingResult
  final case class ShortLiteral(i:Short) extends ParsingResult
  final case class ByteLiteral(i:Byte) extends ParsingResult
  final case class BigDecimalLiteral(i:BigDecimal) extends ParsingResult
  final case class DoubleLiteral(d:Double) extends ParsingResult
  final case class StringLiteral(s:String) extends ParsingResult
  final case class JsonFLiteral[F[_[_]]](jsonF:F[JsonF]) extends ParsingResult
  final case class Aliases(aliases:Set[AvroFQN] Refined NonEmpty) extends ParsingResult
  final case class EnumSymbols(symbols:ListSet[AvroName] Refined NonEmpty) extends ParsingResult
  final case class FieldDefinition(discoveredTypes: Map[String, Nu[AvroType]], meta:AvroRecordFieldMetaData, avroType: Nu[AvroType]) extends ParsingResult
  final case class FieldDefinitions(refs:Map[String, Nu[AvroType]], fields:List[FieldDefinition]) extends ParsingResult
  final case class AvroTypeResult(refs:Map[String, Nu[AvroType]],avroType: Nu[AvroType]) extends ParsingResult


  /*
  Some explanation is waranted here. So IF we already had dependent functiontypes this algebra would look something like this
  Algebra[JsonF, AvroParsingContext => AvroParsingContext.Out]

  So this is what's happening here. Due to the nature of schemas (being possibly infinite) you can't quite use a CoAlgebraM because what will happen is Nu[M[A]] => M[Nu[A]] which forces
  the entire Nu, thus probably running infinitely. Greg Pfeil pointed this out together with the solution https://github.com/sellout/recursion-schemes-cookbook/tree/drafts/AttributeGrammars
  rather then outputing the result directly in the fold, we output a function from some context to our result type. This allows us to effectively pass parameters INTO the fold.

  Now about the either. We unfold a JsonF into an AvroType which is substantially richer than JsonF. This means we do NOT have 1 to 1 mappings from the members of the JsonF GADT to the AvroType one.
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
      ParsingContext => M[ParsingResult]
    ] = jsonF => {

        type Out = ParsingContext => M[ParsingResult]


        def refine[A, P](a: A)(implicit validate:Validate[A, P]) = M.fromEither(refineV[P](a).left.map(RefinementError))

        def fieldAsOpt[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: ParsingContext
        )(
          f: PartialFunction[ParsingResult,M[T]]
        ):M[Option[T]] =
          fields
            .get(field)
            .map(eval => eval(parsingContext))
            .traverse(
              m => m.flatMap(
                x => if (f.isDefinedAt(x)) f(x) else  M.raiseError[T](UnexpectedParsingResult(parsingContext, expected, x))
              )
            )

        def fieldAs[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: ParsingContext
        )(
          f: PartialFunction[ParsingResult,M[T]]
        ):M[T] =
          fieldAsOpt[T](fields)(field, expected)(parsingContext)(f).flatMap {
            case Some(x) => M.pure(x)
            case None => M.raiseError[T](UnknownFieldError(field, fields.keySet))
          }


        jsonF match {
          case json:JsonFNull[Out]         =>  {
            case LiteralDefinition(_, _) => M.pure(NullLiteral())
            case DefaultDefinition(_ ,_) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNull[F[JsonF]]())))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFTrue[Out]         =>  {
            case LiteralDefinition(_, _) => M.pure(BooleanLiteral(true))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFTrue[F[JsonF]]())))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFFalse[Out]        =>  {
            case LiteralDefinition(_, _) => M.pure(BooleanLiteral(false))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFFalse[F[JsonF]]())))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberBigDecimal[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(BigDecimalLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberBigDecimal[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberDouble[Out] =>  {
            case LiteralDefinition(_, _) => M.pure(DoubleLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberDouble[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberBigInt[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(BigIntLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberBigInt[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberLong[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(LongLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberLong[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberInt[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(IntLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberInt[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberShort[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(ShortLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberShort[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFNumberByte[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(ByteLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberByte[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition")))
          }
          case json:JsonFString[Out]       =>  {
            case TypePosition(_, _, parents, refs)    => {
              if (parents.contains(json.value))
                M.pure(
                  AvroTypeResult(
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
                  case Some(x) => M.pure[ParsingResult](AvroTypeResult(refs, x))
                  case None => M.raiseError[ParsingResult](UnkownSchemaReference(json.value, refs.keys.toSet))
                }
            }
            case LiteralDefinition(_, _) => M.pure(StringLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFString[F[JsonF]](json.value))))
            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("LiteralDefinition", "DefaultDefinition", "TypePosition")))
          }
          // Can be a Union if in type position, or a list of fields if in the fields property
          case json:JsonFArray[Out] => {
            case context@TypePosition(history, ns, parents, refs) => {
              json.values
                .map(parse => parse(TypePosition(HistEntry(context, json) :: history, ns, parents, refs)))
                .traverse(
                  m => m.flatMap {
                    case x:AvroTypeResult => M.pure[AvroTypeResult](x)
                    case e:ParsingResult  => M.raiseError[AvroTypeResult](UnexpectedParsingResult(context, "AvroType", e))
                  }
                )
                .map(
                  memberDiscoveries => {
                    val (refs, members) = memberDiscoveries.foldRight((Map.empty[String, Nu[AvroType]], List.empty[Nu[AvroType]]))(
                      (member, agg) => {
                        (agg._1 ++ member.refs, member.avroType :: agg._2)
                      }
                    )

                    AvroTypeResult(refs, typeBirec.embed(AvroUnionType(members)))
                  }
                )
            }
            case context@RecordFieldListDefinition(history, ns, parents, refs) =>
              json.values.foldRight(
                M.pure((Map.empty[String, Nu[AvroType]], List.empty[FieldDefinition]))
              )(
                (parse, m) => m.flatMap(
                  tpl => {
                    parse(RecordFieldDefinition(HistEntry(context, json) :: history, ns,parents, refs ++ tpl._1)).flatMap {
                      case f: FieldDefinition => M.pure((tpl._1 ++ f.discoveredTypes, f :: tpl._2 ))
                      case e => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](UnexpectedParsingResult(context, "field definition",  e))
                    }
                  }
                )
              ).map(
                tpl => FieldDefinitions(tpl._1, tpl._2)
              )
            case context@DefaultDefinition(history, ns) =>
              json.values.traverse(
                parse => parse(DefaultDefinition(HistEntry(context, json) :: history, ns)).flatMap {
                  case jl: JsonFLiteral[F] => M.pure(jl.jsonF)
                  case x => M.raiseError[F[JsonF]](UnexpectedParsingResult(DefaultDefinition(HistEntry(context, json) :: history, ns), "default jsonF of array", x))
                }
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFArray(values))))

            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("DefaultDefinition", "RecordFieldListDefinition", "TypePosition")))
          }
          case json:JsonFObject[Out] => {
            case context@TypePosition(history, ns, parents, refs) =>
              for {
                  typeString <- fieldAs(json.fields)("type", "avro complex type String literal")(LiteralDefinition(HistEntry(context, json) :: history, ns)){
                        case StringLiteral(s) => M.pure(s)
                      }


                  discoveryTypeTuple <- typeString match {
                    case "array" =>
                      fieldAs(json.fields)("items", "array items type definition")(TypePosition(HistEntry(context, json) :: history, ns, parents, refs)) {
                        case AvroTypeResult(discovered, resType) => M.pure(discovered, AvroArrayType(resType): AvroType[Nu[AvroType]])
                      }
                    case "map" =>
                      fieldAs(json.fields)("values", "map value type definition")(TypePosition(HistEntry(context, json) :: history, ns, parents, refs)){
                        case AvroTypeResult(discovered, resType) => M.pure(discovered, AvroMapType(resType):AvroType[Nu[AvroType]])
                      }
                    case "fixed" => for {
                      namespaceLocal <- fieldAsOpt(json.fields)("namespace", "fixed namespace String literal")(LiteralDefinition(HistEntry(context, json) :: history, ns)){
                        case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                      }

                      namespace = namespaceLocal <+> ns.headOption

                      histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)

                      name <- fieldAs(json.fields)("name", "fixed name String literal")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case StringLiteral(s) => refine[String, AvroValidName](s)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      doc <- fieldAsOpt(json.fields)("doc", "fixed doc String literal")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases", "fixed aliases")(AliasesDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      length <- fieldAs(json.fields)("length", "fixed length")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case IntLiteral(i) => refine[Int, Positive](i)
                        case ShortLiteral(i) => refine[Int, Positive](i)
                        case ByteLiteral(i) => refine[Int, Positive](i)
                      }



                      avroType = AvroFixedType[Nu[AvroType]](namespace, name, doc, aliases, length):AvroType[Nu[AvroType]]

                    } yield (refs ++ Map(fqn.value -> typeBirec.embed(avroType)), avroType)
                    case "enum" => for {
                      namespaceLocal <- fieldAsOpt(json.fields)("namespace", "enum namespace")(LiteralDefinition(HistEntry(context, json) :: history, ns)) {
                        case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                      }

                      namespace = namespaceLocal <+> ns.headOption

                      histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)

                      name <- fieldAs(json.fields)("name", "enum name")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case StringLiteral(s) => refine[String, AvroValidName](s)
                      }

                      doc <- fieldAsOpt(json.fields)("doc", "enum doc")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases", "enum aliases")(AliasesDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      symbols <- fieldAs(json.fields)("symbols", "enum symbols")(EnumSymbolDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                        case EnumSymbols(symbols) => M.pure(symbols)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType =AvroEnumType[Nu[AvroType]](namespace, name, doc, aliases, symbols):AvroType[Nu[AvroType]]

                    } yield (refs ++ Map(fqn.value -> typeBirec.embed(avroType)), avroType)

                    case "record" => {
                      for {
                        namespaceLocal <- fieldAsOpt(json.fields)("namespace", "record namespace")(LiteralDefinition(HistEntry(context, json) :: history, ns)) {
                          case StringLiteral(s) => refine[String, AvroValidNamespace](s)
                        }

                        namespace = namespaceLocal <+> ns.headOption

                        histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)


                        name <- fieldAs(json.fields)("name", "record name")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                          case StringLiteral(s) => refine[String, AvroValidName](s)
                        }

                        doc <- fieldAsOpt(json.fields)("doc", "record doc")(LiteralDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                          case StringLiteral(s) => M.pure(s)
                        }

                        aliases <- fieldAsOpt(json.fields)("aliases", "record aliases")(AliasesDefinition(HistEntry(context, json) :: history, histNamespaces)) {
                          case Aliases(aliases) => M.pure(aliases)
                        }

                        fqn = Util.constructFQN(namespace, name).value

                        fieldsDefinition <- fieldAs(json.fields)("fields", "record fields")(
                          RecordFieldListDefinition(HistEntry(context, json) :: history, histNamespaces, parents + fqn, refs)
                        ) {
                          case x:FieldDefinitions => M.pure(x)
                        }

                        fields = fieldsDefinition.fields.map(x => x.meta -> x.avroType).toListMap()

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

                      } yield ((refs ++ fieldsDefinition.refs) + (fqn -> typeBirec.embed(avroType)), avroType)
                    }
                  }
                } yield AvroTypeResult(discoveryTypeTuple._1, typeBirec.embed(discoveryTypeTuple._2))
            case context@RecordFieldDefinition(history, ns, parents, refs) =>
              for {
                name <- fieldAs(json.fields)("name", "field name")(LiteralDefinition(HistEntry(context, json) :: history, ns)) {
                  case StringLiteral(s) => refine[String, AvroValidName](s)
                }

                doc <- fieldAsOpt(json.fields)("doc", "field doc")(LiteralDefinition(HistEntry(context, json) :: history, ns)) {
                  case StringLiteral(s) => M.pure(s)
                }

                aliases <- fieldAsOpt(json.fields)("aliases", "field aliases")(AliasesDefinition(HistEntry(context, json) :: history, ns)) {
                  case Aliases(aliases) => M.pure(aliases)
                }

                orderOpt <- fieldAsOpt(json.fields)("order", "field order")(LiteralDefinition(HistEntry(context, json) :: history, ns)) {
                    case StringLiteral(s) => s match {
                      case "ignore" => M.pure[AvroRecordSortOrder](ARSOIgnore)
                      case "ascending" => M.pure[AvroRecordSortOrder](ARSOAscending)
                      case "descending" => M.pure[AvroRecordSortOrder](ARSODescending)
                      case _ => M.raiseError[AvroRecordSortOrder](UnknownSortOrder(s))
                    }
                }

                order <- M.pure(orderOpt.getOrElse(ARSOIgnore))

                avroTypeResult <- fieldAs(json.fields)("type", "field type")(TypePosition(HistEntry(context, json) :: history, ns, parents, refs)){
                  case x:AvroTypeResult => M.pure(x)
                }


                defaultAvroValue <- fieldAsOpt(json.fields)("default", "default value")(DefaultDefinition(HistEntry(context, json) :: history, ns)) {
                  case jl:JsonFLiteral[F] => {
                    val parser = jsonBirec.cata(jl.jsonF)(parseAvroDatumAlgebra[M])
                    parser(typeBirec.project(avroTypeResult.avroType))
                  }
                }

              } yield FieldDefinition(refs ++ avroTypeResult.refs, AvroRecordFieldMetaData(name, doc, defaultAvroValue, order, aliases), avroTypeResult.avroType):ParsingResult


            case context@DefaultDefinition(history, ns) =>
              json.fields.traverse(
                parse => parse(DefaultDefinition(HistEntry(context, json) :: history, ns)).flatMap {
                  case jl:JsonFLiteral[F] => M.pure(jl.jsonF)
                  case x => M.raiseError[F[JsonF]](UnexpectedParsingResult(context, "default jsonF of object", x))
                }
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFObject(values))))


            case context: ParsingContext => M.raiseError(InvalidParserState(context, Set("DefaultDefinition", "TypePostion", "RecordFieldDefinition")))
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
    case json:JsonFNumberByte[_] => {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberShort[_] => {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberInt[_] => {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberLong[_] => {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberBigInt[_] => _ => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnrepresentableError(json))
    case json:JsonFNumberDouble[_] => {
      case schema:AvroFloatType[Nu[AvroType]] =>
        if (json.value == json.value.toFloat.toDouble) //FIXME this doesn't feel right. will this actually yield sensible floats always?
          M.pure(valueBirec.embed(AvroFloatValue(schema, json.value.toFloat)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, schema))
      case schema:AvroDoubleType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroDoubleValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnexpectedTypeError(json, errSchema))
    }
    case json:JsonFNumberBigDecimal[_] => _ =>  M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](UnrepresentableError(json))
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
    result <- resultFunction(ParsingContext.root)
    out <- result match {
      case AvroTypeResult(_, x) => M.pure(x)
      case x:ParsingResult => M.raiseError[Nu[AvroType]](UnexpectedParsingResult(ParsingContext.root, "An AvroType represented by the passed Schema", x))
    }

  } yield out


}
