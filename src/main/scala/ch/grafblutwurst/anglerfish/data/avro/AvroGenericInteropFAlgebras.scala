package ch.grafblutwurst.anglerfish.data.avro


import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import ch.grafblutwurst.anglerfish.core.refinedExtensions.Refinement._
import ch.grafblutwurst.anglerfish.core.stdLibExtensions.ListSyntax._
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.MonadErrorSyntax._
import eu.timepit.refined._
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.auto._
import eu.timepit.refined.collection._
import eu.timepit.refined.numeric._
import matryoshka._
import matryoshka.data.Nu
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

//TODO: default support
//TODO: record reference support


//BUG: will crash if used on in-code generated GenericRecord. I suspect that the change from String to avro.util.UTF8 happens during serialization. Will have to build in some sort of runtime type mappings
object AvroGenericInteropFAlgebras {

  def avroValueToGenericRepr[M[_], F[_[_]], T: Typeable ](
    avroValue:F[AvroValue[Nu[AvroType], ?]]
  )(
    implicit 
      rec:Recursive.Aux[F[AvroValue[Nu[AvroType], ?]], AvroValue[Nu[AvroType], ?]],
      M: MonadError[M, Throwable]
  ):M[T] =
    for {
      anyFolded <- rec.cataM(avroValue)(avroValueToGenericRepr[M])
      t <- Typeable[T].cast(anyFolded).fold(
        M.raiseError[T](new RuntimeException("Could not downcast to expected Type. Now your T should depend what you pass in as a AvroValue (e.g. AvroRecordValue should be GenericRecord). Sadly I have not found a way to do this dependently typed yet"))
      )(
        x => M.pure(x)
      )
    } yield t
    

  def avroTypeToGenericSchema[M[_]: MonadError[?[_], Throwable]](avroType: Nu[AvroType]):M[Schema] =  avroType.cataM(avroTypeToSchema[M])


  private[this] def handleAvroAliasesJavaSet[M[_], A, P](jSetO: java.util.Set[A])(implicit M:MonadError[M, Throwable], validateEv:Validate[A,P]): M[OptionalNonEmptySet[A Refined P]] = {
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
    
    Traverse[Option].sequence(inverse) match {
      case Left(err) => M.raiseError(new RuntimeException(err)) //FIXME when I do error types
      case Right(x) => M.pure(x)
    }
        
  }


  /**
    * This Coalgebra lets you unfold a org.apache.avro.Schema instance into the recursive AvroType Datastructure. This is needed to later unfold value types into the AvroValue Structure
    * e.g. schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)
    * 
  **/
  //FIXME needs (recursive) reference handling
  def avroSchemaToInternalType[M[_]](implicit M:MonadError[M, Throwable]):CoalgebraM[M, AvroType, Schema] = (schema:Schema) => schema.getType match {
    case Type.NULL => M.pure(AvroNullType())
    case Type.BOOLEAN => M.pure(AvroBooleanType())
    case Type.INT => M.pure(AvroIntType())
    case Type.LONG => M.pure(AvroLongType())
    case Type.FLOAT => M.pure(AvroFloatType())
    case Type.DOUBLE => M.pure(AvroDoubleType())
    case Type.BYTES => M.pure(AvroBytesType())
    case Type.STRING => M.pure(AvroStringType())
    case Type.RECORD => for {
      namespace <- Option(schema.getNamespace).traverse(ns => refineME[M, Throwable, String, AvroValidNamespace](ns)(new RuntimeException(_)))
      name <-  refineME[M, Throwable, String, AvroValidName](schema.getName)(new RuntimeException(_))
      doc = Option(schema.getDoc)
      aliases <- handleAvroAliasesJavaSet[M, String, AvroValidNamespace](schema.getAliases)
      fields <- schema.getFields.asScala.toList.traverse(
        fld => for {
          name <- refineME[M, Throwable, String, AvroValidName](fld.name)(new RuntimeException(_))
          aliases <- handleAvroAliasesJavaSet[M, String, AvroValidNamespace](fld.aliases)
          fldDoc = Option(fld.doc)
          sortOrder = Option(fld.order()).map[AvroRecordSortOrder] {
            case Order.ASCENDING => ARSOAscending
            case Order.DESCENDING => ARSODescending
            case Order.IGNORE => ARSOIgnore
          }.getOrElse(ARSOAscending)
          field = AvroRecordFieldMetaData(name, doc, None, sortOrder, aliases) -> fld.schema
        } yield field
      ).map(_.toListMap)
    } yield AvroRecordType(namespace, name, doc, aliases, fields)


    case Type.ENUM => for {
      namespace <- Option(schema.getNamespace).traverse(ns => refineME[M, Throwable, String, AvroValidNamespace](ns)(new RuntimeException(_)))
      name <-  refineME[M, Throwable, String, AvroValidName](schema.getName)(new RuntimeException(_))
      symbolsLS <- schema.getEnumSymbols.asScala.toList.traverse(symbol => refineME[M, Throwable, String, AvroValidName](symbol)(new RuntimeException(_))).map(_.toListSet)
      symbols <- refineME[M, Throwable, ListSet[String Refined AvroValidName], NonEmpty](symbolsLS)(new RuntimeException(_))
      aliases <- handleAvroAliasesJavaSet[M, String, AvroValidNamespace](schema.getAliases)
    } yield AvroEnumType(namespace, name, Option(schema.getDoc), aliases, symbols)
      
    case Type.ARRAY => M.pure(AvroArrayType(schema.getElementType))
    case Type.MAP => M.pure(AvroMapType(schema.getValueType))
    case Type.UNION => M.pure(AvroUnionType(schema.getTypes.asScala.toList))
    case Type.FIXED => for {
      nameSpace <- Option(schema.getNamespace).traverse( ns => refineME[M, Throwable, String, AvroValidNamespace](ns)(new RuntimeException(_)))
      name <- refineME[M, Throwable, String, AvroValidName](schema.getName)(new RuntimeException(_))
      length <- refineME[M, Throwable, Int, Positive](schema.getFixedSize)(new RuntimeException(_))
      aliases <- handleAvroAliasesJavaSet[M, String, AvroValidNamespace](schema.getAliases)
    } yield AvroFixedType(nameSpace, name, Option(schema.getDoc), aliases, length) 
  }

 //FIXME: Requires refactor pass (error reporting, think if we can streamline the reverse union matching )
  /**
    * This Coalgebra lets you unfold a Pair of (AvroType, Any) into Either[Error, F[AvroValue]] where F is your chosen Fixpoint. The instance of Any has to correlate to what the AvroType represents. e.g. if the Schema represents a Record, any has to be a GenericData.Record.
    * usage: (schemaInternal, deserializedGenRec).anaM[Fix[AvroValue[Fix[AvroType], ?]]](AvroAlgebra.avroGenericReprToInternal[Fix]) though I'll prorbably make some convinience functions down the line to make this a bit easier
  **/
  def avroGenericReprToInternal[M[_]](implicit birec:Birecursive.Aux[Nu[AvroType], AvroType], M:MonadError[M, Throwable]):CoalgebraM[M, AvroValue[Nu[AvroType], ?], (Nu[AvroType], Any)] = (tp:(Nu[AvroType], Any)) => { 

    def checkValueType[T: Typeable](rawJavaValue:Any, schema:AvroType[Nu[AvroType]])(implicit tt:TypeTag[T]):M[(Nu[AvroType], T)] = for {
      raw <-  if (rawJavaValue != null) M.pure(rawJavaValue) else M.raiseError(new RuntimeException(s"item at $schema was null on selection"))
      out <- Typeable[T].cast(raw) match {
        case Some(t) => M.pure((birec.embed(schema), t))
        case None => M.raiseError(new RuntimeException(s"Could not cast value $raw to ${tt.tpe}"))
      }
    } yield out

    def extractForNextRecursionStep(componentSchema:AvroType[Nu[AvroType]]): Any => M[(Nu[AvroType], Any)] = componentSchema match {
          case AvroNullType() => _ =>  M.pure((birec.embed(componentSchema), null))
          case AvroBooleanType() => value => checkValueType[Boolean](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroIntType() => value => checkValueType[Int](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroLongType() => value => checkValueType[Long](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroFloatType() => value => checkValueType[Float](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroDoubleType() => value => checkValueType[Double](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroBytesType() => value => checkValueType[java.nio.ByteBuffer](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case AvroStringType() => value => checkValueType[org.apache.avro.util.Utf8](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroRecordType[_] => value =>  checkValueType[GenericData.Record](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroEnumType[_] => value => checkValueType[GenericData.EnumSymbol](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroArrayType[_] => value => checkValueType[GenericData.Array[Any]](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroMapType[_] => value => checkValueType[java.util.HashMap[String, Any]](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroUnionType[_] => value => M.pure((birec.embed(componentSchema), value)) //In case of the union it needs to be handled downstream. a union is represented as just a java.lang.object and can actually be any of it's members
          case _: AvroFixedType[_] => value => checkValueType[GenericData.Fixed](value, componentSchema).map(tpl => (tpl._1, tpl._2:Any))
          case _: AvroRecursionType[_] => _ => M.raiseError(new RuntimeException("Encountered a recursive type where we shouldn't have one frankly"))
        }

    def checkUnion(unionSchema: AvroUnionType[Nu[AvroType]])(unionValue:Any)(typeLabel: String)(filter: PartialFunction[AvroType[Nu[AvroType]], Boolean]):M[AvroValue[Nu[AvroType], (Nu[AvroType], Any)]] =
      unionSchema.members.map(x => birec.project(x))
        .find(x => if(filter.isDefinedAt(x)) filter(x) else false)
        .fold(
          M.raiseError[AvroValue[Nu[AvroType], (Nu[AvroType], Any)]](new RuntimeException(s"Unresolved Union: ${unionSchema.members} did not contain a $typeLabel"))
        )(
          memberSchema => extractForNextRecursionStep(memberSchema)(unionValue).map(tpl => AvroUnionValue(unionSchema, tpl) )
        )


    val outerSchema = birec.project(tp._1) match {
      case recursive:AvroRecursionType[Nu[AvroType]] => birec.project(recursive.lazyType)
      case x: AvroType[Nu[AvroType]] => x
    }

    outerSchema match {
      case _:AvroRecursionType[_] => M.raiseError(new RuntimeException("Encountered a recursive type where we shouldn't have one frankly"))
      case nullSchema:AvroNullType[Nu[AvroType]] => M.pure(AvroNullValue(nullSchema))
      case booleanSchema:AvroBooleanType[Nu[AvroType]] => checkValueType[Boolean](tp._2, outerSchema).map(tpl => AvroBooleanValue(booleanSchema, tpl._2))
      case intSchema:AvroIntType[Nu[AvroType]] => checkValueType[Int](tp._2, outerSchema).map(tpl => AvroIntValue(intSchema, tpl._2))
      case longSchema:AvroLongType[Nu[AvroType]] => checkValueType[Long](tp._2, outerSchema).map(tpl => AvroLongValue(longSchema, tpl._2))
      case floatSchema:AvroFloatType[Nu[AvroType]] => checkValueType[Float](tp._2, outerSchema).map(tpl => AvroFloatValue(floatSchema, tpl._2))
      case doubleSchema:AvroDoubleType[Nu[AvroType]] => checkValueType[Double](tp._2, outerSchema).map(tpl => AvroDoubleValue(doubleSchema, tpl._2))
      case bytesSchema:AvroBytesType[Nu[AvroType]] => checkValueType[java.nio.ByteBuffer](tp._2, outerSchema).map(tpl => AvroBytesValue(bytesSchema, tpl._2.array().toVector))
      case stringSchema:AvroStringType[Nu[AvroType]] => checkValueType[org.apache.avro.util.Utf8](tp._2, outerSchema).map(tpl => AvroStringValue(stringSchema, tpl._2.toString))
      case rec:AvroRecordType[Nu[AvroType]] => for {
        genRec <- checkValueType[GenericData.Record](tp._2, rec).map(_._2)
        fields <- rec.fields.toList.traverse (
          fieldDefinition => extractForNextRecursionStep(birec.project(fieldDefinition._2))(genRec.get(fieldDefinition._1.name)).map(tpl => fieldDefinition._1.name -> tpl)
        ).map(_.toListMap)
      } yield AvroRecordValue(rec, fields)

      case enumSchema:AvroEnumType[Nu[AvroType]] => checkValueType[GenericData.EnumSymbol](tp._2, outerSchema).map(tpl => AvroEnumValue(enumSchema, tpl._2.toString))
      case arraySchema:AvroArrayType[Nu[AvroType]] => {
        val refinement = extractForNextRecursionStep(birec.project(arraySchema.items))
        val elemsE = checkValueType[GenericData.Array[Any]](tp._2, arraySchema).map(_._2.iterator.asScala.toList)
        elemsE.flatMap(elem => Traverse[List].traverse(elem)(refinement)).map( elems => AvroArrayValue(arraySchema, elems) )
      }
      case mapSchema: AvroMapType[Nu[AvroType]] => {
        val refinement = extractForNextRecursionStep(birec.project(mapSchema.values))
        val elemsE = checkValueType[java.util.HashMap[String, Any]](tp._2, mapSchema).map(_._2.asScala.toMap)
        elemsE.flatMap(elem => Traverse[Map[String,?]].traverse(elem)(refinement)).map( elems => AvroMapValue(mapSchema, elems) )
      }
      case unionSchema: AvroUnionType[Nu[AvroType]] => {
        //In case of a union we need to reverse match the whole thing. take the value and match it's type against the described members in the unionSchema. if there's a fit apply it if not throw an error
        //For types that can occur multiple times in a union (Fixed, Enum, Record) we'll match on namespace and name
        tp._2 match {
          case null => checkUnion(unionSchema)(null)("Null") {
              case AvroNullType() => true
            }

          case unionVal:Boolean => checkUnion(unionSchema)(unionVal)("Boolean") {
              case AvroBooleanType() => true
            }

          case unionVal:Int => checkUnion(unionSchema)(unionVal)("Int") {
            case AvroIntType() => true
          }

          case unionVal:Long => checkUnion(unionSchema)(unionVal)("Long") {
            case AvroLongType() => true
          }

          case unionVal:Float => checkUnion(unionSchema)(unionVal)("Float") {
            case AvroFloatType() => true
          }

          case unionVal:Double => checkUnion(unionSchema)(unionVal)("Double") {
            case AvroDoubleType() => true
          }

          case unionVal:java.nio.ByteBuffer => checkUnion(unionSchema)(unionVal)("BytesType") {
            case AvroBytesType() => true
          }

          case unionVal:org.apache.avro.util.Utf8 => checkUnion(unionSchema)(unionVal)("String") {
            case AvroStringType() => true
          }

          case unionVal:GenericData.Record => checkUnion(unionSchema)(unionVal)(unionVal.getSchema.getFullName) {
            case namedType:AvroRecordType[_] => unionVal.getSchema.getFullName === Util.constructFQN(namedType.namespace, namedType.name).value
          }

          case unionVal:GenericData.EnumSymbol => checkUnion(unionSchema)(unionVal)(unionVal.getSchema.getFullName) {
            case namedType:AvroEnumType[_] => unionVal.getSchema.getFullName === Util.constructFQN(namedType.namespace, namedType.name).value
          }

          case unionVal:GenericData.Array[_] => checkUnion(unionSchema)(unionVal)("ArrayType") {
            case _:AvroArrayType[_] => true
          }


          case unionVal:java.util.HashMap[_, _] => checkUnion(unionSchema)(unionVal)("MapType") {
            case _:AvroMapType[_] => true
          }

          case unionVal:GenericData.Fixed => checkUnion(unionSchema)(unionVal)(unionVal.getSchema.getFullName) {
            case namedType:AvroFixedType[_] => unionVal.getSchema.getFullName === Util.constructFQN(namedType.namespace, namedType.name).value
          }

        }
      }
      case fixedSchema: AvroFixedType[Nu[AvroType]] => checkValueType[GenericData.Fixed](tp._2, outerSchema).map(tpl => AvroFixedValue(fixedSchema, tpl._2.bytes.toVector))
    }
  }


  //FIXME: what about aliases?
  /**
    * This Algebra allows to fold a AvroType back down to a schema. so that a hylomorphism with avroSchemaToInternalType should yield the same schema again
  **/
  def avroTypeToSchema[M[_]](implicit M:MonadError[M, Throwable]):AlgebraM[M, AvroType, Schema] = (avroType:AvroType[Schema]) => avroType match {
    case AvroRecursionType(_, _) => M.raiseError(new NotImplementedError("Recursive References in Avro Schema folding not yet supported"))
    case AvroNullType() => M.pure(Schema.create(Schema.Type.NULL))
    case AvroBooleanType() => M.pure(Schema.create(Schema.Type.BOOLEAN))
    case AvroIntType() => M.pure(Schema.create(Schema.Type.INT))
    case AvroLongType() => M.pure(Schema.create(Schema.Type.LONG))
    case AvroFloatType() => M.pure(Schema.create(Schema.Type.FLOAT))
    case AvroDoubleType() => M.pure(Schema.create(Schema.Type.DOUBLE))
    case AvroBytesType() => M.pure(Schema.create(Schema.Type.BYTES))
    case AvroStringType() => M.pure(Schema.create(Schema.Type.STRING))
    case rec:AvroRecordType[Schema] => {
      val flds = rec.fields.foldRight(M.pure(List.empty[Schema.Field])) (
        (elemKv, lstM) => lstM.flatMap( lst => {
          val elemMeta = elemKv._1
          val elemSchema = elemKv._2

          val sortOrder = elemMeta.order match {
            case ARSOIgnore => Schema.Field.Order.IGNORE
            case ARSOAscending => Schema.Field.Order.ASCENDING
            case ARSODescending => Schema.Field.Order.DESCENDING
          }
          elemMeta.default.map(_.cataM[M, Any](avroValueToGenericRepr)).getOrElse(M.pure(null)).map(
            default => {
              val fldInstance = new Schema.Field(elemMeta.name, elemSchema, elemMeta.doc.orNull, default, sortOrder)
              elemMeta.aliases.foreach(
                _.foreach(alias => fldInstance.addAlias(alias.value))
              )
              fldInstance :: lst
            }
          )
        }
        )
      )

      flds.flatMap(
        fields => {
          M.tryThunk {
            val schemaInstance = Schema.createRecord(rec.name, rec.doc.orNull, rec.namespace.map(_.value).orNull, false, fields.asJava)
            rec.aliases.foreach(
              _.foreach(alias => schemaInstance.addAlias(alias.value))
            )
            schemaInstance
          }
        }
      )

    }
    case enum:AvroEnumType[_] => {
      M.tryThunk {
        val schemaInstance = Schema.createEnum(enum.name, enum.doc.orNull, enum.namespace.map(_.value).getOrElse(""), enum.symbols.toList.map(_.value).asJava)
          enum.aliases.foreach(
            _.foreach(alias => schemaInstance.addAlias(alias.value))
          )
        schemaInstance
      }
    }
    case arr:AvroArrayType[Schema] => M.pure(Schema.createArray(arr.items))
    case map:AvroMapType[Schema] => M.pure(Schema.createMap(map.values))
    case unionT:AvroUnionType[Schema] => M.pure(Schema.createUnion(unionT.members.asJava))
    case fixed:AvroFixedType[_] => M.tryThunk{
      val schemaInstance = Schema.createFixed(fixed.name, fixed.doc.orNull, fixed.namespace.map(_.value).getOrElse(""), fixed.length.value)
      fixed.aliases.foreach(
        _.foreach(alias => schemaInstance.addAlias(alias.value))
      )
      schemaInstance
    }
  }


  /**
    * This is the algebra to fold the AvroValue back down to the generic representation which should be writable by Avoros serializers. note that Any is going to be dependent what kind avroValue you pass in.
  **/
  //FIXME inefficient as we foold schemas down for every element that's probably not necessary
  def avroValueToGenericRepr[M[_]](implicit birec:Birecursive.Aux[Nu[AvroType], AvroType], M:MonadError[M, Throwable]):AlgebraM[M, AvroValue[Nu[AvroType], ?], Any] = (avroValue:AvroValue[Nu[AvroType],Any]) => avroValue match {
    case AvroNullValue(_) => M.pure(null)
    case AvroBooleanValue(_, b) => M.pure(b)
    case AvroIntValue(_, i) => M.pure(i)
    case AvroLongValue(_, l) => M.pure(l)
    case AvroFloatValue(_ , f) => M.pure(f)
    case AvroDoubleValue(_, d) => M.pure(d)
    case AvroBytesValue(_ , bs) => M.pure(bs.toArray)
    case AvroStringValue(_, s) => M.tryThunk { new org.apache.avro.util.Utf8(s) }
    case AvroRecordValue(schema, flds) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema(M))
      genRec <- M.tryThunk { new GenericData.Record(avroSchema) }
    } yield flds.foldLeft(genRec)( (rec, fld) => {rec.put(fld._1, fld._2); rec})
    case AvroEnumValue(schema, symbol) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema(M))
      genEnum <- M.tryThunk { new GenericData.EnumSymbol(avroSchema, symbol) }
    } yield genEnum
      //new GenericData.Array(birec.cata(birec.embed(schema))(avroTypeToSchema), items.asJava)
    case AvroArrayValue(schema, items) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema(M))
      genArray <- M.tryThunk { new GenericData.Array(avroSchema, items.asJava) }
    } yield genArray
    case AvroMapValue(_, values) =>  M.pure(values.asJava)
    case AvroUnionValue(_, member) => M.pure(member)
    case AvroFixedValue(schema, bytes) => for {
      avroSchema <- birec.cataM(birec.embed(schema))(avroTypeToSchema(M))
      genArray <- M.tryThunk { new GenericData.Fixed(avroSchema, bytes.toArray) }
    } yield genArray
  }
  


}
