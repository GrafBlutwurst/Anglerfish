package ch.grafblutwurst.anglerfish.data.avro


import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import eu.timepit.refined._
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.auto._
import eu.timepit.refined.collection._
import eu.timepit.refined.numeric._
import matryoshka._
import matryoshka.implicits._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field.Order
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericData, _}
import org.apache.avro.io.DecoderFactory
import scalaz.Scalaz._
import scalaz._
import shapeless.Typeable
import shapeless.Typeable._

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, ListSet}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}


/**
  *  This is a collection of Algebras to use with FP-Avro:
  *  e.g. to get from a schema to an internal representation
**/

//TODO: CLEAN UP
//TODO: reformulate to applicative error?
//TODO: default support
//TODO: record reference support
trait AvroGenericInteropFAlgebras[Eff[_]] {
  def parseAvroSchema(schemaString:String):Eff[Schema]
  def unfoldAvroSchema[F[_[_]]](schema:Schema)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Eff[F[AvroType]]
  def decodeAvroJsonRepr(schema:Schema)(avroJsonString:String):Eff[Any] //FIXME: do we want to expose Any here?
  //FIXME: do we actually need birec here?
  def unfoldGenericRepr[F[_[_]]](schema:F[AvroType])(genRepr:Any)(implicit schemaBirec:Birecursive.Aux[F[AvroType], AvroType], valueCorec:Corecursive.Aux[F[AvroValue[F[AvroType], ?]], AvroValue[F[AvroType], ?]]):Eff[F[AvroValue[F[AvroType], ?]]]
  def foldToGenericRepr[F[_[_]]](avroValue:F[AvroValue[F[AvroType], ?]])(implicit birec:Birecursive.Aux[F[AvroType], AvroType], valueBirec:Birecursive.Aux[F[AvroValue[F[AvroType], ?]], AvroValue[F[AvroType], ?]]):Eff[Any]
  def foldToAvroSchema[F[_[_]]](schema:F[AvroType])(implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Eff[Schema]
}


//BUG: will crash if used on in-code generated GenericRecord. I suspect that the change from String to avro.util.UTF8 happens during serialization. Will have to build in some sort of runtime type mappings
object AvroGenericInteropFAlgebras {

  private implicit class RichTry[T](t:Try[T]){
    def toEither:Either[Throwable,T] = t.transform(s => Success(Right(s)), f => Success(Left(f))).get
  }

  //FIXME: UGHHHH use shims here at some point
  def catsMeInstance[E, ME[_]: cats.MonadError[?[_], E]](ef: String => E) = new AvroGenericInteropFAlgebras[ME] {


    def parseAvroSchema(schemaString:String):ME[Schema] =
      Try {(new Schema.Parser).parse(schemaString) } match {
        case Success(v) => cats.MonadError[ME, E].pure(v)
        case Failure(t) => cats.MonadError[ME, E].raiseError(ef(t.toString))
      }

    def decodeAvroJsonRepr(schema:Schema)(avroJsonString:String):ME[Any] = Try {
      val decoder = DecoderFactory.get().jsonDecoder(schema, avroJsonString)
      new GenericDatumReader[Any](schema, schema).read(null, decoder)
    } match {
      case Success(v) => cats.MonadError[ME, E].pure(v)
      case Failure(t) => cats.MonadError[ME, E].raiseError(ef(t.toString))
    }


    def unfoldAvroSchema[F[_[_]]](schema:Schema)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]):ME[F[AvroType]] =
      schema.anaM[F[AvroType]](AvroGenericInteropFAlgebras.avroSchemaToInternalType).fold(
        err => cats.MonadError[ME, E].raiseError(ef(err)),
        fpVal => cats.MonadError[ME, E].pure(fpVal)
      )

    def unfoldGenericRepr[F[_[_]]](schema:F[AvroType])(genRepr:Any)(implicit schemaBirec:Birecursive.Aux[F[AvroType], AvroType],  valueCorec:Corecursive.Aux[F[AvroValue[F[AvroType], ?]], AvroValue[F[AvroType], ?]]):ME[F[AvroValue[F[AvroType], ?]]] =
      (schema, genRepr).anaM[F[AvroValue[F[AvroType], ?]]](AvroGenericInteropFAlgebras.avroGenericReprToInternal[F]).fold(
        err => cats.MonadError[ME, E].raiseError(ef(err)),
        fpVal => cats.MonadError[ME, E].pure(fpVal)
      )

    def foldToGenericRepr[F[_[_]]](avroValue:F[AvroValue[F[AvroType], ?]])(implicit birec:Birecursive.Aux[F[AvroType], AvroType], valueBirec:Birecursive.Aux[F[AvroValue[F[AvroType], ?]], AvroValue[F[AvroType], ?]]):ME[Any] =
      valueBirec.cataM(avroValue)(avroValueToGenericRepr).fold(
        err => cats.MonadError[ME, E].raiseError(ef(err)),
        fpVal => cats.MonadError[ME, E].pure(fpVal)
      )

    def foldToAvroSchema[F[_[_]]](schema:F[AvroType])(implicit birec:Birecursive.Aux[F[AvroType], AvroType]):ME[Schema] =
      birec.cataM(schema)(avroTypeToSchema).fold(
        err => cats.MonadError[ME, E].raiseError(ef(err)),
        fpVal => cats.MonadError[ME, E].pure(fpVal)
      )

  }


  //FIXME: Maybe rewrite this to some generic Error Structure (MonadError, maybe something weaker???)

  //FIXME: This needs a rewrite. ListSet and Set need to be sorted out see toList.toSet to get to a proper Set rather than a ListSet. but Set has no Traversable instance, consider just using ListSet
  private[this] def handleAvroAliasesJavaSet[P, A](jSetO: java.util.Set[A])(implicit validateEv:Validate[A,P]): Either[String, OptionalNonEmptySet[A Refined P]] = {
    val inverse = Option(jSetO).flatMap(
      jset => {
        val scalaLSet = jset.asScala.toList.foldLeft(ListSet.empty[A])(_+_)
        val validatedSetE:Either[String, Set[A Refined P]] = Traverse[ListSet].traverse(scalaLSet)(refineV[P](_))
        val optValSet:Either[String, Option[Set[A Refined P] Refined NonEmpty]] = validatedSetE.map(
            possiblyEmptySet => refineV[NonEmpty](possiblyEmptySet.toList.toSet) match {
              case Left(_) => None
              case Right(set) => Some(set)
            }
          )
        Traverse[Either[String, ?]].sequence(optValSet)
      }
    )
    
    Traverse[Option].sequence(inverse)
        
  }


  /**
    * This Coalgebra lets you unfold a org.apache.avro.Schema instance into the recursive AvroType Datastructure. This is needed to later unfold value types into the AvroValue Structure
    * e.g. schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)
    * 
  **/
  def avroSchemaToInternalType:CoalgebraM[Either[String,?], AvroType, Schema] = (schema:Schema) => schema.getType match {
    case Type.NULL => Right(AvroNullType())
    case Type.BOOLEAN => Right(AvroBooleanType())
    case Type.INT => Right(AvroIntType())
    case Type.LONG => Right(AvroLongType())
    case Type.FLOAT => Right(AvroFloatType())
    case Type.DOUBLE => Right(AvroDoubleType())
    case Type.BYTES => Right(AvroBytesType())
    case Type.STRING => Right(AvroStringType())
    case Type.RECORD => {
      val nameSpaceOpt = Option(schema.getNamespace)
      val nameS = schema.getName
      val doc = Option(schema.getDoc)

   
      val fieldsE = Traverse[List].traverse(schema.getFields.asScala.toList)(
        (fld:Field) => {
          val fldNameS =  fld.name
          val fldDoc = Option(fld.doc)
          val fldDefultExpr = None //FIXME: Evaulate Json node to value. Gotta figure out how to type the default value
          val fldSortOrder = Option(fld.order()).map[AvroRecordSortOrder] {
            case Order.ASCENDING => ARSOAscending
            case Order.DESCENDING => ARSODescending
            case Order.IGNORE => ARSOIgnore
          }.getOrElse(ARSOAscending)

          for {
            fldName <- refineV[AvroValidName](fldNameS)
            fldAlias <- handleAvroAliasesJavaSet[AvroValidNamespace, String](fld.aliases)
          } yield  AvroRecordFieldMetaData(fldName, fldDoc, fldDefultExpr, fldSortOrder, fldAlias) -> fld.schema


        }
      ).map(
        _.foldLeft(ListMap.empty[AvroRecordFieldMetaData, Schema]) (
          (map, elem) => map + elem
        )
      )
      
      for {
        nameSpace <- nameSpaceOpt.traverse(refineV[AvroValidNamespace](_))
        name <- refineV[AvroValidName](nameS)
        fields <- fieldsE
        aliases <- handleAvroAliasesJavaSet[AvroValidNamespace, String](schema.getAliases)
      } yield AvroRecordType(nameSpace, name, doc, aliases, fields)
    }

    case Type.ENUM => for {
      nameSpace <- Option(schema.getNamespace).traverse(refineV[AvroValidNamespace](_))
      name <- refineV[AvroValidName](schema.getName)
      symbolsL <- Traverse[List].traverse(schema.getEnumSymbols.asScala.toList)(refineV[AvroValidName](_))
      symbolsE = symbolsL.foldLeft(ListSet.empty[String Refined AvroValidName])(_ + _)
      symbols <- refineV[NonEmpty](symbolsE)
      aliases <- handleAvroAliasesJavaSet[AvroValidNamespace, String](schema.getAliases)
    } yield AvroEnumType(nameSpace, name, Option(schema.getDoc), aliases, symbols)
    case Type.ARRAY => Right(AvroArrayType(schema.getElementType))
    case Type.MAP => Right(AvroMapType(schema.getValueType))
    case Type.UNION => Right(AvroUnionType(schema.getTypes.asScala.toList))
    case Type.FIXED => for {
      nameSpace <- Option(schema.getNamespace).traverse(refineV[AvroValidNamespace](_))
      name <- refineV[AvroValidName](schema.getName)
      length <- refineV[Positive](schema.getFixedSize)
      aliases <- handleAvroAliasesJavaSet[AvroValidNamespace, String](schema.getAliases)
    } yield AvroFixedType(nameSpace, name, Option(schema.getDoc), aliases, length) 
  }

 //FIXME: Requires refactor pass (error reporting, think if we can streamline the reverse union matching )
  /**
    * This Coalgebra lets you unfold a Pair of (AvroType, Any) into Either[Error, F[AvroValue]] where F is your chosen Fixpoint. The instance of Any has to correlate to what the AvroType represents. e.g. if the Schema represents a Record, any has to be a GenericData.Record.
    * usage: (schemaInternal, deserializedGenRec).anaM[Fix[AvroValue[Fix[AvroType], ?]]](AvroAlgebra.avroGenericReprToInternal[Fix]) though I'll prorbably make some convinience functions down the line to make this a bit easier
  **/
  def avroGenericReprToInternal[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):CoalgebraM[Either[String, ?], AvroValue[F[AvroType], ?], (F[AvroType], Any)] = (tp:(F[AvroType], Any)) => { 

    def castValue[T: Typeable](rawJavaValue:Any, schema:AvroType[F[AvroType]])(implicit tt:TypeTag[T]):Either[String, (F[AvroType], T)] = {
      val rawE = if (rawJavaValue != null) Right(rawJavaValue) else Left(s"item at $schema was null on selection")
      rawE.flatMap( 
        raw => Typeable[T].cast(raw) match {
          case Some(t) => Right((birec.embed(schema), t))
          case None => Left(s"Could not cast value $raw to ${tt.tpe}")
        }
      )
    }
    val outerSchema = birec.project(tp._1) 

    def refineInstance(componentSchema:AvroType[F[AvroType]]): Any => Either[String, (F[AvroType], Any)] = componentSchema match {
          case AvroNullType() => _ =>  Right((birec.embed(componentSchema), null))
          case AvroBooleanType() => value => castValue[Boolean](value, componentSchema)
          case AvroIntType() => value => castValue[Int](value, componentSchema)
          case AvroLongType() => value => castValue[Long](value, componentSchema)
          case AvroFloatType() => value => castValue[Float](value, componentSchema)
          case AvroDoubleType() => value => castValue[Double](value, componentSchema)
          case AvroBytesType() => value => castValue[java.nio.ByteBuffer](value, componentSchema)
          case AvroStringType() => value => castValue[org.apache.avro.util.Utf8](value, componentSchema)
          case _: AvroRecordType[_] => value =>  castValue[GenericData.Record](value, componentSchema)
          case _: AvroEnumType[_] => value => castValue[GenericData.EnumSymbol](value, componentSchema)
          case _: AvroArrayType[_] => value => castValue[GenericData.Array[Any]](value, componentSchema)
          case _: AvroMapType[_] => value => castValue[java.util.HashMap[String, Any]](value, componentSchema)
          case _: AvroUnionType[_] => value => Right((birec.embed(componentSchema), value)) //In case of the union it needs to be handled downstream. a union is represented as just a java.lang.object and can actually be any of it's members
          case _: AvroFixedType[_] => value => castValue[GenericData.Fixed](value, componentSchema)
        }


    outerSchema match {
      case nullSchema:AvroNullType[F[AvroType]] => Right(AvroNullValue(nullSchema))
      case booleanSchema:AvroBooleanType[F[AvroType]] => castValue[Boolean](tp._2, outerSchema).map(tpl => AvroBooleanValue(booleanSchema, tpl._2))
      case intSchema:AvroIntType[F[AvroType]] => castValue[Int](tp._2, outerSchema).map(tpl => AvroIntValue(intSchema, tpl._2))
      case longSchema:AvroLongType[F[AvroType]] => castValue[Long](tp._2, outerSchema).map(tpl => AvroLongValue(longSchema, tpl._2))
      case floatSchema:AvroFloatType[F[AvroType]] => castValue[Float](tp._2, outerSchema).map(tpl => AvroFloatValue(floatSchema, tpl._2))
      case doubleSchema:AvroDoubleType[F[AvroType]] => castValue[Double](tp._2, outerSchema).map(tpl => AvroDoubleValue(doubleSchema, tpl._2))
      case bytesSchema:AvroBytesType[F[AvroType]] => castValue[java.nio.ByteBuffer](tp._2, outerSchema).map(tpl => AvroBytesValue(bytesSchema, tpl._2.array.toVector))
      case stringSchema:AvroStringType[F[AvroType]] => castValue[org.apache.avro.util.Utf8](tp._2, outerSchema).map(tpl => AvroStringValue(stringSchema, tpl._2.toString))
      case rec:AvroRecordType[F[AvroType]] =>  {
        val shouldBeRec = castValue[GenericData.Record](tp._2, rec).map(_._2)
        shouldBeRec.flatMap(
          gRec => {
            val lmTraverse = Traverse[ListMap[String, ?]]
            //these are allowed to be hetrogenous
            val fields = lmTraverse.traverse(rec.fields.map( kv => (kv._1.name, (kv._1, kv._2)))) (
              kv => {
                val fieldName = kv._1.name
                val schema:AvroType[F[AvroType]] = birec.project(kv._2)
                val value = gRec.get(fieldName)
                refineInstance(schema)(value)
              }
            )
            fields.map( flds => AvroRecordValue(rec,flds))
          }
        )
      }
      case enumSchema:AvroEnumType[F[AvroType]] => castValue[GenericData.EnumSymbol](tp._2, outerSchema).map(tpl => AvroEnumValue(enumSchema, tpl._2.toString))
      case arraySchema:AvroArrayType[F[AvroType]] => {
        val refinement = refineInstance(birec.project(arraySchema.items))
        val elemsE = castValue[GenericData.Array[Any]](tp._2, arraySchema).map(_._2.iterator.asScala.toList)
        elemsE.flatMap(elem => Traverse[List].traverse(elem)(refinement)).map( elems => AvroArrayValue(arraySchema, elems) )
      }
      case mapSchema: AvroMapType[F[AvroType]] => {
        val refinement = refineInstance(birec.project(mapSchema.values))
        val elemsE = castValue[java.util.HashMap[String, Any]](tp._2, mapSchema).map(_._2.asScala.toMap)
        elemsE.flatMap(elem => Traverse[Map[String,?]].traverse(elem)(refinement)).map( elems => AvroMapValue(mapSchema, elems) )
      }
      case unionSchema: AvroUnionType[F[AvroType]] => {
        //In case of a union we need to reverse match the whole thing. take the value and match it's type against the described members in the unionSchema. if there's a fit apply it if not throw an error
        //For types that can occur multiple times in a union (Fixed, Enum, Record) we'll match on namespace and name
        tp._2 match { //FIXME: Think how we can make this a bit smaller
          case null => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroNullType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a NullType")
            )(
              memberSchema => refineInstance(memberSchema)(null).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Boolean => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroBooleanType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a BooleanType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Int => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroIntType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a IntType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Long => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroLongType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a LongType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Float => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroFloatType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a FloatType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Double => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroDoubleType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a DoubleType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:java.nio.ByteBuffer => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroBytesType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a BytesType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:org.apache.avro.util.Utf8 => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroStringType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a StringType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Record => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case recordType:AvroRecordType[_] => unionVal.getSchema.getNamespace == recordType.namespace.map(_.value).getOrElse("") && unionVal.getSchema.getName == recordType.name.value
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a a Record type allowing this generic record to be properly decoded")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.EnumSymbol => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case enumType:AvroEnumType[_] => unionVal.getSchema.getNamespace == enumType.namespace.map(_.value).getOrElse("") && unionVal.getSchema.getName == enumType.name.value
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an EnumType of the proper namespace and name")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Array[_] => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case _:AvroArrayType[_] => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an ArrayType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:java.util.HashMap[_, _] => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case _:AvroMapType[_] => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a Map Type")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Fixed => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case fixedType:AvroFixedType[_] => unionVal.getSchema.getNamespace == fixedType.namespace.map(_.value).getOrElse("") && unionVal.getSchema.getName == fixedType.name.value
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an EnumType of the proper namespace and name")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

        }
      }
      case fixedSchema: AvroFixedType[F[AvroType]] => castValue[GenericData.Fixed](tp._2, outerSchema).map(tpl => AvroFixedValue(fixedSchema, tpl._2.bytes.toVector))
    }
  }


  //FIXME: what about aliases?
  /**
    * This Algebra allows to fold a AvroType back down to a schema. not that a hylomorphism with avroSchemaToInternalType should yield the same schema again
  **/
  val avroTypeToSchema:AlgebraM[Either[String,?],AvroType, Schema] = (avroType:AvroType[Schema]) => avroType match {
    case AvroNullType() => Right(Schema.create(Schema.Type.NULL))
    case AvroBooleanType() => Right(Schema.create(Schema.Type.BOOLEAN))
    case AvroIntType() => Right(Schema.create(Schema.Type.INT))
    case AvroLongType() => Right(Schema.create(Schema.Type.LONG))
    case AvroFloatType() => Right(Schema.create(Schema.Type.FLOAT))
    case AvroDoubleType() => Right(Schema.create(Schema.Type.DOUBLE))
    case AvroBytesType() => Right(Schema.create(Schema.Type.BYTES))
    case AvroStringType() => Right(Schema.create(Schema.Type.STRING))
    case rec:AvroRecordType[Schema] => {
      val flds = rec.fields.foldRight(List.empty[Schema.Field]) (
        (elemKv, lst) => {
          val elemMeta = elemKv._1
          val elemSchema = elemKv._2

          val sortOrder = elemMeta.order match {
            case ARSOIgnore => Schema.Field.Order.IGNORE
            case ARSOAscending => Schema.Field.Order.ASCENDING
            case ARSODescending => Schema.Field.Order.DESCENDING
          }
          val fldInstance = new Schema.Field(elemMeta.name, elemSchema, elemMeta.doc.getOrElse(null), elemMeta.default.getOrElse(null), sortOrder)
          elemMeta.aliases.foreach(
            _.foreach(alias => fldInstance.addAlias(alias.value))
          )
          fldInstance :: lst
        }
      )

      Try { 
        val schemaInstance = Schema.createRecord(rec.name, rec.doc.getOrElse(null), rec.namespace.map(_.value).getOrElse(""), false, flds.asJava)
        rec.aliases.foreach(
          _.foreach(alias => schemaInstance.addAlias(alias.value))
        )
        schemaInstance
      }.toEither.left.map(_.getMessage)
      
    }
    case enum:AvroEnumType[_] => {
      Try { 
        val schemaInstance = Schema.createEnum(enum.name, enum.doc.getOrElse(null), enum.namespace.map(_.value).getOrElse(""), enum.symbols.toList.map(_.value).asJava)
          enum.aliases.foreach(
            _.foreach(alias => schemaInstance.addAlias(alias.value))
          )
        schemaInstance
      }.toEither.left.map(_.getMessage)
    }
    case arr:AvroArrayType[Schema] => Right(Schema.createArray(arr.items))
    case map:AvroMapType[Schema] => Right(Schema.createMap(map.values))
    case unionT:AvroUnionType[Schema] => Right(Schema.createUnion(unionT.members.asJava))
    case fixed:AvroFixedType[_] => Try{ 
      val schemaInstance = Schema.createFixed(fixed.name, fixed.doc.getOrElse(null), fixed.namespace.map(_.value).getOrElse(""), fixed.length.value)
      fixed.aliases.foreach(
        _.foreach(alias => schemaInstance.addAlias(alias.value))
      )
      schemaInstance
    }.toEither.left.map(_.getMessage)
  }


  /**
    * This is the algebra to fold the AvroValue back down to the generic representation which should be writable by Avoros serializers. note that Any is going to be dependent what kind avroValue you pass in.
  **/
  def avroValueToGenericRepr[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):AlgebraM[Either[String,?], AvroValue[F[AvroType], ?], Any] = (avroValue:AvroValue[F[AvroType],Any]) => avroValue match {
    case AvroNullValue(_) => Right(null)
    case AvroBooleanValue(_, b) => Right(b)
    case AvroIntValue(_, i) => Right(i)
    case AvroLongValue(_, l) => Right(l)
    case AvroFloatValue(_ , f) => Right(f)
    case AvroDoubleValue(_, d) => Right(d)
    case AvroBytesValue(_ , bs) => Right(bs.toArray)
    case AvroStringValue(_, s) => Try { new org.apache.avro.util.Utf8(s) }.toEither.left.map(_.getMessage)
    case AvroRecordValue(schema, flds) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema)
      genRec <- Try { new GenericData.Record(avroSchema) }.toEither.left.map(_.getMessage)
    } yield flds.foldLeft(genRec)( (rec, fld) => {rec.put(fld._1, fld._2); rec})
    case AvroEnumValue(schema, symbol) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema)
      genEnum <- Try { new GenericData.EnumSymbol(avroSchema, symbol) }.toEither.left.map(_.getMessage)
    } yield genEnum
      //new GenericData.Array(birec.cata(birec.embed(schema))(avroTypeToSchema), items.asJava)
    case AvroArrayValue(schema, items) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema)
      genArray <- Try { new GenericData.Array(avroSchema, items.asJava) }.toEither.left.map(_.getMessage)
    } yield genArray
    case AvroMapValue(_, values) =>  Right(values.asJava)
    case AvroUnionValue(_, member) => Right(member)
    case AvroFixedValue(schema, bytes) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema)
      genArray <- Try { new GenericData.Fixed(avroSchema, bytes.toArray) }.toEither.left.map(_.getMessage)
    } yield genArray
  }
  


}
