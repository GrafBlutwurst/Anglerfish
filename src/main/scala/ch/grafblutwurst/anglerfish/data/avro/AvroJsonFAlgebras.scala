package ch.grafblutwurst.anglerfish.data.avro

import java.util.Base64

import scalaz._
import Scalaz._
import ch.grafblutwurst.anglerfish.data.avro.AvroData.{AvroNamespace, _}
import ch.grafblutwurst.anglerfish.data.json.JsonData._
import ch.grafblutwurst.anglerfish.data.json.JsonFAlgebras._
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import ch.grafblutwurst.anglerfish.data.json.implicits._
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.MonadErrorSyntax._
import ch.grafblutwurst.anglerfish.core.stdLibExtensions.ListSyntax._
import ch.grafblutwurst.anglerfish.data.avro.AvroJsonFAlgebras.{FoundUndeclaredFields, MissingDeclaredFields}
import matryoshka.{Algebra, Birecursive, Coalgebra, Corecursive, GAlgebra, GCoalgebra, Recursive}
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
import scalaz.Liskov.>~>

import scala.collection.immutable.{ListMap, ListSet, Queue, Stack}

object AvroJsonFAlgebras {


  sealed trait AvroError extends Throwable{
    override def getMessage: String = Show[AvroError].shows(this)
  }

  object AvroError{
    implicit val show:Show[AvroError] = Show.shows[AvroError] {
      case JsonFError(err) => Show[JsonError].shows(err)
      case EnumError(history, expected, encountered) => s"""EnumError: ${HistEntry.top(history)} expected one of $expected, but found $encountered"""
      case UnionError(history, reason) => s"UnionError: ${HistEntry.top(history)}   $reason"
      case UnionResolutionError(history, reason) => s"UnionResolutionError: ${HistEntry.top(history)}  $reason"
      case MissingDeclaredFields(history, fields) => s"""${HistEntry.top(history)}  Missing Declared Fields: ${fields.mkString(",")}"""
      case FoundUndeclaredFields(history, fields) => s"""${HistEntry.top(history)}  Found Undeclared Fields: ${fields.mkString(",")}"""
      case NoDefaultValue(history, field) => s"""${HistEntry.top(history)} required field $field neither had a value nor a default"""
      case FixedError(history, expected, encounteredLength) => s"""${HistEntry.top(history)}  expected length $expected, but found $encounteredLength"""
      case UnexpectedTypeError(history, contextSchema, encountered) => s"""UnexpectedType: ${HistEntry.top(history)} expected $contextSchema found ${JsonF.flatDescribe(encountered)}"""
      case UnrepresentableError(history, reason) => s"""${HistEntry.top(history)}  found JSON Type not representable by AVRO $reason"""
      case UnkownSchemaReference(history, errorReference, knownReferences) => s"""${HistEntry.top(history)} Found named reference $errorReference but it's currently not known (${knownReferences.mkString(",")})"""
      case UnexpectedParsingResult(history, expected, got) => s"""${HistEntry.top(history)} Parsing Step resulted in unexpected result $got. Expected was: $expected"""
      case InvalidParserState(history, validStates) => s"""${HistEntry.top(history)} allowed states at this point are ${validStates.mkString(",")}"""
      case UnknownFieldError(history, field) => s"""${HistEntry.top(history)} $field is missing """
      case RefinementError(history, error) => s"Refinement Error: ${HistEntry.top(history)} $error"
      case UnknownSortOrder(history, error) => s"${HistEntry.top(history)} $error is not a valid sort order"
    }
  }

  final case class JsonFError(err: JsonError) extends AvroError

  sealed trait AvroDatumError extends AvroError {
    val history: List[HistEntry]
  }
  final case class EnumError(history: List[HistEntry], expected:AvroEnumType[Nu[AvroType]], encountered:String) extends AvroDatumError
  final case class UnionError(history: List[HistEntry], reason:String) extends AvroDatumError
  final case class UnionResolutionError(history: List[HistEntry], reason:String) extends AvroDatumError
  final case class MissingDeclaredFields(history: List[HistEntry], fields:Set[String]) extends AvroDatumError
  final case class FoundUndeclaredFields(history: List[HistEntry], fields:Set[String]) extends AvroDatumError
  final case class NoDefaultValue(history: List[HistEntry], field:String) extends AvroDatumError
  final case class FixedError(history: List[HistEntry], expected:AvroFixedType[Nu[AvroType]], encounteredLength:Int) extends AvroDatumError
  final case class UnexpectedTypeError(history: List[HistEntry], contextSchema: AvroType[Nu[AvroType]], encountered: JsonF[_]) extends AvroDatumError
  final case class UnrepresentableError(history: List[HistEntry], reason:String) extends AvroDatumError


  sealed trait AvroSchemaError extends AvroError{
    val history: List[HistEntry]
  }
  final case class UnkownSchemaReference(history: List[HistEntry], errorReference:String, knownReferences: Set[String]) extends AvroSchemaError
  final case class UnexpectedParsingResult(history: List[HistEntry], expected:String, got:ParsingResult) extends AvroSchemaError
  final case class InvalidParserState(history: List[HistEntry], validStates:Set[String]) extends AvroSchemaError
  final case class UnknownFieldError(history: List[HistEntry], field:String) extends AvroSchemaError
  final case class RefinementError(history: List[HistEntry], error:String) extends AvroSchemaError
  final case class UnknownSortOrder(history: List[HistEntry], error:String) extends AvroSchemaError



  private[this] def decodeBytes(s:String):Vector[Byte] = Base64.getDecoder.decode(s).toVector

  final case class HistEntry(parseStep:String, jsonF: JsonF[_], positionDescriptor:String)
  object HistEntry{

    def top(hist:List[HistEntry]):String = {
      val path = hist.reverse.map(_.positionDescriptor).mkString(".")
      hist.headOption.map(histEntry => s"in parse-step ${histEntry.parseStep}, on jsonF ${JsonF.flatDescribe(histEntry.jsonF)}, at json position ${path}").getOrElse("")
    }

    def all(hist:List[HistEntry]):String = hist
      .map(histEntry => s"in parse-step ${histEntry.parseStep}, on jsonF ${JsonF.flatDescribe(histEntry.jsonF)}, at json position ${histEntry.positionDescriptor}")
      .mkString("\n")
  }

  sealed trait SchemaParsingContext {
    val history: List[HistEntry]
    val encolsingNamespace: List[AvroNamespace]
    val parseStepDescriptor:String
  }
  object SchemaParsingContext{



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
  final case class TypePosition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "TypePosition"
  }
  final case class RecordFieldDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "RecordFieldDefinition"
  }
  final case class RecordFieldListDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace], parentTypes:Set[String], schemaReferences:Map[String, Nu[AvroType]]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "RecordFieldListDefinition"
  }
  final case class DefaultDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "DefaultDefinition"
  }
  final case class LiteralDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "LiteralDefinition"
  }
  final case class AliasesDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "AliasesDefinition"
  }
  final case class DefaultValueDefition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "DefaultValueDefition"
  }
  final case class EnumSymbolDefinition(history: List[HistEntry], encolsingNamespace: List[AvroNamespace]) extends SchemaParsingContext{
    val parseStepDescriptor:String = "EnumSymbolDefinition"
  }



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


  def AvroValueToJsonF(implicit corec:Corecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]):Algebra[AvroValue[Nu[AvroType], ?],Fix[JsonF]] = ???


  def AvroSchemaToJsonF(implicit corec:Corecursive.Aux[Fix[JsonF], JsonF], valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]):GCoalgebra[Free[JsonF, ?], JsonF, Nu[AvroType]] = {
    def freeJsonFString(s:String):Free[JsonF, Nu[AvroType]] = Free.liftF(JsonFString(s))
    def freeJsonFInt(i:Int):Free[JsonF, Nu[AvroType]] = Free.liftF(JsonFNumberInt(i))
    def freeJsonFArray(lst:List[Free[JsonF, Nu[AvroType]]]):Free[JsonF, Nu[AvroType]] = Free(JsonFArray(lst))
    def freeJsonFObject(lstMap:ListMap[String, Free[JsonF, Nu[AvroType]]]): Free[JsonF, Nu[AvroType]] = Free(JsonFObject(lstMap))

    nu =>
      nu.project match {
        case AvroNullType() => JsonFString("null")
        case AvroBooleanType() => JsonFString("boolean")
        case AvroIntType() => JsonFString("int")
        case AvroLongType() => JsonFString("long")
        case AvroFloatType() => JsonFString("float")
        case AvroDoubleType() => JsonFString("double")
        case AvroBytesType() => JsonFString("bytes")
        case AvroStringType() => JsonFString("string")
        case AvroRecordType(namespace, name, doc, aliases, fields) =>
          JsonFObject(
            ListMap("type" -> freeJsonFString("record")) ++
              namespace.map(ns => ListMap("namespace" -> freeJsonFString(ns.value))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap("name" -> freeJsonFString(name.value)) ++
              doc.map(doc => ListMap("doc" -> freeJsonFString(doc))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              aliases.map(
                aliasesSet => ListMap("aliases" -> freeJsonFArray(aliasesSet.toList.map(s => freeJsonFString(s.value))))
              ).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap(
                "fields" ->
                  freeJsonFArray(
                    fields.toList.map(
                      kv =>
                        freeJsonFObject(
                          ListMap("name" -> freeJsonFString(kv._1.name)) ++
                          kv._1.doc.map(doc => ListMap("doc" -> freeJsonFString(doc))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
                          //kv._1.default.map(defaultValue => ListMap("default" -> Free.liftF[JsonF, Nu[AvroType]](valueBirec.cata(defaultValue)(AvroValueToJsonF).unFix))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
                          ListMap(
                            "order" -> (
                              kv._1.order match {
                                case ARSOAscending => freeJsonFString("ascending")
                                case ARSODescending => freeJsonFString("descending")
                                case ARSOIgnore => freeJsonFString("ignore")
                              }
                              )
                          ) ++
                          kv._1.aliases.map(
                            aliasesSet => ListMap("aliases" -> freeJsonFArray(aliasesSet.toList.map(s => freeJsonFString(s.value))))
                          ).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
                          ListMap("type" -> Free.pure[JsonF, Nu[AvroType]](kv._2))
                        )
                    )
                  )
              )
          )
        case AvroEnumType(namespace, name, doc, aliases, symbols) =>
          JsonFObject(
            ListMap("type" -> freeJsonFString("enum")) ++
              namespace.map(ns => ListMap("namespace" -> freeJsonFString(ns))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap("name" -> freeJsonFString(name.value)) ++
              doc.map(doc => ListMap("doc" -> freeJsonFString(doc))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              aliases.map(
                aliasesSet =>
                  ListMap(
                    "aliases" -> freeJsonFArray(aliasesSet.toList.map(s => freeJsonFString(s.value)))

                  )
              ).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap("symbols" -> freeJsonFArray(symbols.map(s => freeJsonFString(s)).toList))
          )
        case AvroArrayType(items) =>
          JsonFObject(
            ListMap("type" -> freeJsonFString("array")) ++
            ListMap("items" -> Free.pure[JsonF, Nu[AvroType]](items))
          )
        case AvroMapType(values) =>
          JsonFObject(
            ListMap("type" -> freeJsonFString("array")) ++
              ListMap("items" -> Free.pure[JsonF, Nu[AvroType]](values))
          )
        case AvroUnionType(members) => JsonFArray(
          members.map(Free.pure[JsonF, Nu[AvroType]])
        )
        case AvroFixedType(namespace, name, doc, aliases, length) =>
          JsonFObject(
            ListMap("type" -> freeJsonFString("fixed")) ++
              namespace.map(ns => ListMap("namespace" -> freeJsonFString(ns))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap("name" -> freeJsonFString(name.value)) ++
              doc.map(doc => ListMap("doc" -> freeJsonFString(doc))).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              aliases.map(
                aliasesSet =>
                  ListMap(
                    "aliases" -> freeJsonFArray(aliasesSet.toList.map(s => freeJsonFString(s.value)))

                  )
              ).getOrElse(ListMap.empty[String, Free[JsonF, Nu[AvroType]]]) ++
              ListMap("length" -> freeJsonFInt(length))
          )
        case AvroRecursionType(fqn, _) => JsonFString(fqn)


      }


  }

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
  def parseAvroSchemaAlgebra[M[_], F[_[_]], E](implicit M:MonadError[M, E], jsonBirec:Birecursive.Aux[F[JsonF], JsonF], typeBirec:Birecursive.Aux[Nu[AvroType], AvroType], valueBirec:Birecursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]], liskov: E >~> AvroError):
    Algebra[
      JsonF,
      SchemaParsingContext => M[ParsingResult]
    ] = jsonF => {

        type Out = SchemaParsingContext => M[ParsingResult]


        def refine[A, P](parsingContext:SchemaParsingContext)(a: A)(implicit validate:Validate[A, P]) = M.fromEither(refineV[P](a).left.map( e => liskov(RefinementError(parsingContext.history, e))))

        def fieldAsOpt[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: SchemaParsingContext
        )(
          f: PartialFunction[ParsingResult,M[T]]
        ):M[Option[T]] =
          fields
            .get(field)
            .map(eval => eval(parsingContext))
            .traverse(
              m => m.flatMap(
                x => if (f.isDefinedAt(x)) f(x) else  M.raiseError[T](liskov(UnexpectedParsingResult(parsingContext.history, expected, x)))
              )
            )

        def fieldAs[T](
          fields:ListMap[String, Out]
        )(
          field:String, expected:String
        )(
          parsingContext: SchemaParsingContext
        )(
          f: PartialFunction[ParsingResult,M[T]]
        ):M[T] =
          fieldAsOpt[T](fields)(field, expected)(parsingContext)(f).flatMap {
            case Some(x) => M.pure(x)
            case None => M.raiseError[T](liskov(UnknownFieldError(parsingContext.history, field)))
          }


        jsonF match {
          case json:JsonFNull[Out]         =>  {
            case LiteralDefinition(_, _) => M.pure(NullLiteral())
            case DefaultDefinition(_ ,_) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNull[F[JsonF]]())))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFTrue[Out]         =>  {
            case LiteralDefinition(_, _) => M.pure(BooleanLiteral(true))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFTrue[F[JsonF]]())))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFFalse[Out]        =>  {
            case LiteralDefinition(_, _) => M.pure(BooleanLiteral(false))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFFalse[F[JsonF]]())))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberBigDecimal[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(BigDecimalLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberBigDecimal[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberDouble[Out] =>  {
            case LiteralDefinition(_, _) => M.pure(DoubleLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberDouble[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberBigInt[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(BigIntLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberBigInt[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberLong[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(LongLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberLong[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberInt[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(IntLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberInt[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberShort[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(ShortLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberShort[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFNumberByte[Out]    =>  {
            case LiteralDefinition(_, _) => M.pure(ByteLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFNumberByte[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition"))))
          }
          case json:JsonFString[Out]       =>  {
            case context@TypePosition(_, _, parents, refs)    => {
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
                  case None => M.raiseError[ParsingResult](liskov(UnkownSchemaReference(context.history, json.value, refs.keySet)))
                }
            }
            case LiteralDefinition(_, _) => M.pure(StringLiteral(json.value))
            case DefaultDefinition(_, _) => M.pure(JsonFLiteral(jsonBirec.embed(JsonFString[F[JsonF]](json.value))))
            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("LiteralDefinition", "DefaultDefinition", "TypePosition"))))
          }
          // Can be a Union if in type position, or a list of fields if in the fields property
          case json:JsonFArray[Out] => {
            case context@TypePosition(history, ns, parents, refs) => {
              json
                .values
                .zipWithIndex
                .map(
                  tpl => {
                    val idx = tpl._2
                    val parse = tpl._1
                    parse(TypePosition(HistEntry(context.parseStepDescriptor, json, s"Array[$idx]") :: history, ns, parents, refs))
                  }
                )
                .traverse(
                  m => m.flatMap {
                    case x:AvroTypeResult => M.pure[AvroTypeResult](x)
                    case e:ParsingResult  => M.raiseError[AvroTypeResult](liskov(UnexpectedParsingResult(context.history, "AvroType", e)))
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
                    parse(RecordFieldDefinition(HistEntry(context.parseStepDescriptor, json, "RecordFieldList") :: history, ns,parents, refs ++ tpl._1)).flatMap {
                      case f: FieldDefinition => M.pure((tpl._1 ++ f.discoveredTypes, f :: tpl._2 ))
                      case e => M.raiseError[(Map[String, Nu[AvroType]], List[FieldDefinition])](liskov(UnexpectedParsingResult(context.history, "field definition",  e)))
                    }
                  }
                )
              ).map(
                tpl => FieldDefinitions(tpl._1, tpl._2)
              )
            case context@DefaultDefinition(history, ns) =>
              json.values.traverse(
                parse => parse(DefaultDefinition(HistEntry(context.parseStepDescriptor, json, "DefaultDefinition") :: history, ns)).flatMap {
                  case jl: JsonFLiteral[F] => M.pure(jl.jsonF)
                  case x => M.raiseError[F[JsonF]](liskov(UnexpectedParsingResult(HistEntry(context.parseStepDescriptor, json, "DefaultDefinition") :: history, "default jsonF of array", x)))
                }
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFArray(values))))

            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("DefaultDefinition", "RecordFieldListDefinition", "TypePosition"))))
          }
          case json:JsonFObject[Out] => {
            case context@TypePosition(history, ns, parents, refs) =>
              for {
                  typeString <- fieldAs(json.fields)("type", "avro complex type String literal")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "type") :: history, ns)){
                        case StringLiteral(s) => M.pure(s)
                      }


                  discoveryTypeTuple <- typeString match {
                    case "array" =>
                      fieldAs(json.fields)("items", "array items type definition")(TypePosition(HistEntry(context.parseStepDescriptor, json, "items") :: history, ns, parents, refs)) {
                        case AvroTypeResult(discovered, resType) => M.pure(discovered, AvroArrayType(resType): AvroType[Nu[AvroType]])
                      }
                    case "map" =>
                      fieldAs(json.fields)("values", "map value type definition")(TypePosition(HistEntry(context.parseStepDescriptor, json, "values") :: history, ns, parents, refs)){
                        case AvroTypeResult(discovered, resType) => M.pure(discovered, AvroMapType(resType):AvroType[Nu[AvroType]])
                      }
                    case "fixed" => for {
                      namespaceLocal <- fieldAsOpt(json.fields)("namespace", "fixed namespace String literal")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "namespace") :: history, ns)){
                        case StringLiteral(s) => refine[String, AvroValidNamespace](context)(s)
                      }

                      namespace = namespaceLocal <+> ns.headOption

                      histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)

                      name <- fieldAs(json.fields)("name", "fixed name String literal")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "name") :: history, histNamespaces)) {
                        case StringLiteral(s) => refine[String, AvroValidName](context)(s)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      doc <- fieldAsOpt(json.fields)("doc", "fixed doc String literal")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "doc") :: history, histNamespaces)) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases", "fixed aliases")(AliasesDefinition(HistEntry(context.parseStepDescriptor, json, "aliases") :: history, histNamespaces)) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      length <- fieldAs(json.fields)("length", "fixed length")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "length") :: history, histNamespaces)) {
                        case IntLiteral(i) => refine[Int, Positive](context)(i)
                        case ShortLiteral(i) => refine[Int, Positive](context)(i)
                        case ByteLiteral(i) => refine[Int, Positive](context)(i)
                      }



                      avroType = AvroFixedType[Nu[AvroType]](namespace, name, doc, aliases, length):AvroType[Nu[AvroType]]

                    } yield (refs ++ Map(fqn.value -> typeBirec.embed(avroType)), avroType)
                    case "enum" => for {
                      namespaceLocal <- fieldAsOpt(json.fields)("namespace", "enum namespace")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "namepace") :: history, ns)) {
                        case StringLiteral(s) => refine[String, AvroValidNamespace](context)(s)
                      }

                      namespace = namespaceLocal <+> ns.headOption

                      histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)

                      name <- fieldAs(json.fields)("name", "enum name")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "name") :: history, histNamespaces)) {
                        case StringLiteral(s) => refine[String, AvroValidName](context)(s)
                      }

                      doc <- fieldAsOpt(json.fields)("doc", "enum doc")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "doc") :: history, histNamespaces)) {
                        case StringLiteral(s) => M.pure(s)
                      }

                      aliases <- fieldAsOpt(json.fields)("aliases", "enum aliases")(AliasesDefinition(HistEntry(context.parseStepDescriptor, json, "aliases") :: history, histNamespaces)) {
                        case Aliases(aliases) => M.pure(aliases)
                      }

                      symbols <- fieldAs(json.fields)("symbols", "enum symbols")(EnumSymbolDefinition(HistEntry(context.parseStepDescriptor, json, "symbols") :: history, histNamespaces)) {
                        case EnumSymbols(symbols) => M.pure(symbols)
                      }

                      fqn = Util.constructFQN(namespace, name)

                      avroType =AvroEnumType[Nu[AvroType]](namespace, name, doc, aliases, symbols):AvroType[Nu[AvroType]]

                    } yield (refs ++ Map(fqn.value -> typeBirec.embed(avroType)), avroType)

                    case "record" => {
                      for {
                        namespaceLocal <- fieldAsOpt(json.fields)("namespace", "record namespace")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "namespace") :: history, ns)) {
                          case StringLiteral(s) => refine[String, AvroValidNamespace](context)(s)
                        }

                        namespace = namespaceLocal <+> ns.headOption

                        histNamespaces = namespaceLocal.map(nsl => nsl :: ns).getOrElse(ns)


                        name <- fieldAs(json.fields)("name", "record name")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "name") :: history, histNamespaces)) {
                          case StringLiteral(s) => refine[String, AvroValidName](context)(s)
                        }

                        doc <- fieldAsOpt(json.fields)("doc", "record doc")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "doc") :: history, histNamespaces)) {
                          case StringLiteral(s) => M.pure(s)
                        }

                        aliases <- fieldAsOpt(json.fields)("aliases", "record aliases")(AliasesDefinition(HistEntry(context.parseStepDescriptor, json, "aliases") :: history, histNamespaces)) {
                          case Aliases(aliases) => M.pure(aliases)
                        }

                        fqn = Util.constructFQN(namespace, name).value

                        fieldsDefinition <- fieldAs(json.fields)("fields", "record fields")(
                          RecordFieldListDefinition(HistEntry(context.parseStepDescriptor, json, "fields") :: history, histNamespaces, parents + fqn, refs)
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
                name <- fieldAs(json.fields)("name", "field name")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "name") :: history, ns)) {
                  case StringLiteral(s) => refine[String, AvroValidName](context)(s)
                }

                doc <- fieldAsOpt(json.fields)("doc", "field doc")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "doc") :: history, ns)) {
                  case StringLiteral(s) => M.pure(s)
                }

                aliases <- fieldAsOpt(json.fields)("aliases", "field aliases")(AliasesDefinition(HistEntry(context.parseStepDescriptor, json, "aliases") :: history, ns)) {
                  case Aliases(aliases) => M.pure(aliases)
                }

                orderOpt <- fieldAsOpt(json.fields)("order", "field order")(LiteralDefinition(HistEntry(context.parseStepDescriptor, json, "order") :: history, ns)) {
                    case StringLiteral(s) => s match {
                      case "ignore" => M.pure[AvroRecordSortOrder](ARSOIgnore)
                      case "ascending" => M.pure[AvroRecordSortOrder](ARSOAscending)
                      case "descending" => M.pure[AvroRecordSortOrder](ARSODescending)
                      case _ => M.raiseError[AvroRecordSortOrder](liskov(UnknownSortOrder(context.history, s)))
                    }
                }

                order <- M.pure(orderOpt.getOrElse(ARSOIgnore))

                avroTypeResult <- fieldAs(json.fields)("type", "field type")(TypePosition(HistEntry(context.parseStepDescriptor, json, "type") :: history, ns, parents, refs)){
                  case x:AvroTypeResult => M.pure(x)
                }


                defaultAvroValue <- fieldAsOpt(json.fields)("default", "default value")(DefaultDefinition(HistEntry(context.parseStepDescriptor, json, "default") :: history, ns)) {
                  case jl:JsonFLiteral[F] => {
                    val parser = jsonBirec.cata(jl.jsonF)(parseAvroDatumAlgebra[M, E])
                    parser(DatumParsingContext(Nil, typeBirec.project(avroTypeResult.avroType)))
                  }
                }

              } yield FieldDefinition(refs ++ avroTypeResult.refs, AvroRecordFieldMetaData(name, doc, defaultAvroValue, order, aliases), avroTypeResult.avroType):ParsingResult


            case context@DefaultDefinition(history, ns) =>
              json.fields.traverse(
                parse => parse(DefaultDefinition(HistEntry(context.parseStepDescriptor, json, "defaultDefinition") :: history, ns)).flatMap {
                  case jl:JsonFLiteral[F] => M.pure(jl.jsonF)
                  case x => M.raiseError[F[JsonF]](liskov(UnexpectedParsingResult(context.history, "default jsonF of object", x)))
                }
              ).map(values => JsonFLiteral(jsonBirec.embed(JsonFObject(values))))


            case context: SchemaParsingContext => M.raiseError(liskov(InvalidParserState(context.history, Set("DefaultDefinition", "TypePostion", "RecordFieldDefinition"))))
          }
    }
  }

  private def expandRecursiveReference(schema:AvroType[Nu[AvroType]])(implicit typeBirec:Birecursive.Aux[Nu[AvroType], AvroType]):AvroType[Nu[AvroType]] = schema match {
    case member: AvroRecursionType[Nu[AvroType]] => typeBirec.project(member.lazyType)
    case x: AvroType[Nu[AvroType]] => x
  }


  private[this] def selectUnionMemberByName[M[_], E](history:List[HistEntry], members:List[Nu[AvroType]], selector:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    M: MonadError[M, E],
    liskov: E >~> UnionResolutionError
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
      case Nil => M.raiseError(liskov(UnionResolutionError(history, "Unknown Union Branch " + selector)))
      case x::xs => M.raiseError(liskov(UnionResolutionError(history, "Could not disambiguate" + selector)))
    }


  final case class DatumParsingContext(history:List[HistEntry], schemaType: AvroType[Nu[AvroType]])

  def parseAvroDatumAlgebra[M[_], E](
                                            implicit M:MonadError[M, E],
                                            typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
                                            valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
                                            jsonBirec:Birecursive.Aux[Fix[JsonF], JsonF],
                                            liskov: E >~> AvroDatumError
                                          ):Algebra[
                                              JsonF,
                                              DatumParsingContext => M[Fix[AvroValue[Nu[AvroType], ?]]]
                                            ] = {
    case json:JsonFNull[_] => context => context.schemaType match {
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
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnionError(context.history, "Found null object at Union position, but Union did not contain null")))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFTrue[_] => context => context.schemaType match {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, true)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFFalse[_] => context => context.schemaType match {
      case schema:AvroBooleanType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBooleanValue(schema, false)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberByte[_] => context => context.schemaType match {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberShort[_] => context => context.schemaType match {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberInt[_] => context => context.schemaType match {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case schema:AvroIntType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroIntValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberLong[_] => context => context.schemaType match {
      case schema:AvroLongType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroLongValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberBigInt[_] => context => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnrepresentableError(context.history, "json bigint")))
    case json:JsonFNumberDouble[_] => context => context.schemaType match {
      case schema:AvroFloatType[Nu[AvroType]] =>
        if (json.value == json.value.toFloat.toDouble) //FIXME this doesn't feel right. will this actually yield sensible floats always?
          M.pure(valueBirec.embed(AvroFloatValue(schema, json.value.toFloat)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, schema, json)))
      case schema:AvroDoubleType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroDoubleValue(schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFNumberBigDecimal[_] => context =>  M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnrepresentableError(context.history, "json bigdecimal")))
    case json:JsonFString[_] => context => context.schemaType match {
      case schema:AvroBytesType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroBytesValue(schema, decodeBytes(json.value))))
      case schema:AvroStringType[Nu[AvroType]] => M.pure(valueBirec.embed(AvroStringValue(schema, json.value)))
      case schema:AvroFixedType[Nu[AvroType]] => {
        val bytes = decodeBytes(json.value)
        if (bytes.length == schema.length.value)
          M.pure(valueBirec.embed(AvroFixedValue(schema, bytes)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(FixedError(context.history, schema, bytes.length)))
      }
      case schema:AvroEnumType[Nu[AvroType]] =>
        if (schema.symbols.map(_.value).contains(json.value))
          M.pure(valueBirec.embed(AvroEnumValue(schema, json.value)))
        else
          M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(EnumError(context.history, schema, json.value)))
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
    }
    case json:JsonFArray[DatumParsingContext => M[Fix[AvroValue[Nu[AvroType], ?]]]] => context => context.schemaType match {
      case schema:AvroArrayType[Nu[AvroType]] =>
        json
          .values
          .zipWithIndex
          .traverse(
            tpl => {
              val f = tpl._1
              val pContext = DatumParsingContext(
                HistEntry("JsonFArray/AvroArrayType", json, "Array[${tpl._2}]") :: context.history, 
                expandRecursiveReference(typeBirec.project(schema.items))
              )
              f(pContext)
            }
          )
          .map(elems => valueBirec.embed(AvroArrayValue(schema, elems)))
      case errSchema:AvroType[Nu[AvroType]] => {
        M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
      }
    }
    case json:JsonFObject[DatumParsingContext => M[Fix[AvroValue[Nu[AvroType], ?]]]] => context => context.schemaType match {
      case schema:AvroMapType[Nu[AvroType]] => {
        json
          .fields
          .toList
          .traverse(
            kv =>{
              val f = kv._2
              val pContext = DatumParsingContext(HistEntry("Map Field Parsing", json, kv._1) :: context.history, expandRecursiveReference(typeBirec.project(schema.values)))
              f(pContext).map(avroValue => kv._1 -> avroValue)
            }
          )
          .map(elems => valueBirec.embed(AvroMapValue(schema, elems.toListMap())))
      }
      case schema:AvroUnionType[Nu[AvroType]] => for {
          member <-
            json.fields.foldLeft(M.pure(Option.empty[(String, DatumParsingContext => M[Fix[AvroValue[Nu[AvroType], ?]]])]))(
              (optM, elem) => optM.flatMap {
                case None => M.pure(Some(elem))
                case Some(_) => M.raiseError[Option[(String, DatumParsingContext=> M[Fix[AvroValue[Nu[AvroType], ?]]])]](liskov(UnionError(context.history, "Union representation object must contain exactly 1 field")))
              }
            ).flatMap {
              case None => M.raiseError[(String, DatumParsingContext => M[Fix[AvroValue[Nu[AvroType], ?]]])](liskov(UnionError(context.history, "Union representation object must contain exactly 1 field")))
              case Some(x) => M.pure(x)
            }

          memberType <- selectUnionMemberByName[M, E](context.history, schema.members, member._1)

          value <- member._2(DatumParsingContext(HistEntry("union member parsing", json, member._1) :: context.history, memberType))
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
          _ <- if (missingRecFields.isEmpty) M.pure(()) else M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(MissingDeclaredFields(context.history, missingRecFields)))
          _ <- if (unmappedJsonFields.isEmpty) M.pure(()) else M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(FoundUndeclaredFields(context.history, unmappedJsonFields)))


          fields <- schema.fields.map(kv => kv._1.name -> (kv._1, expandRecursiveReference(typeBirec.project(kv._2)))).traverse(
            kv => {
              M.bind(
                M.fromEither(
                  (
                    json
                      .fields
                      .get(kv._1.name)
                      .map(f => 
                        f(
                          DatumParsingContext(HistEntry("parse record field", json, kv._1.name) :: context.history,kv._2)
                        )
                      ) <+> kv._1.default.map(fix => M.pure(fix)))
                    .toRight(liskov(NoDefaultValue(context.history, kv._1.name)))
                )
              )(identity)
            }

          )

          rec = AvroRecordValue(schema, fields)

        } yield valueBirec.embed(rec)
      }
      case errSchema:AvroType[Nu[AvroType]] => M.raiseError[Fix[AvroValue[Nu[AvroType], ?]]](liskov(UnexpectedTypeError(context.history, errSchema, json)))
      
    }

  }





  def parseDatum[M[_]](schema:Nu[AvroType])(avroJsonRepr:String)(
    implicit M:MonadError[M, Throwable],
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]]
  ):M[Fix[AvroValue[Nu[AvroType], ?]]] = for {
    jsonF <- parseJsonF[M,Fix, Throwable](avroJsonRepr)
    resultFunction = jsonF.cata(parseAvroDatumAlgebra[M, Throwable])
    result <- resultFunction(DatumParsingContext(HistEntry("root", jsonF.unFix, "root") :: Nil ,typeBirec.project(schema)))
  } yield result

  def parseSchema[M[_]](schemaString:String)(
    implicit
    typeBirec:Birecursive.Aux[Nu[AvroType], AvroType],
    valueBirec:Birecursive.Aux[Fix[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
    M: MonadError[M, Throwable]
  ):M[Nu[AvroType]] = for {
    jsonF <- parseJsonF[M,Fix, Throwable](schemaString)
    resultFunction = jsonF.cata(parseAvroSchemaAlgebra[M, Fix, Throwable])
    result <- resultFunction(SchemaParsingContext.root)
    out <- result match {
      case AvroTypeResult(_, x) => M.pure(x)
      case x:ParsingResult => M.raiseError[Nu[AvroType]](UnexpectedParsingResult(SchemaParsingContext.root.history, "An AvroType represented by the passed Schema", x))
    }

  } yield out


}
