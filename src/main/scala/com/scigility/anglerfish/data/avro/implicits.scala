package com.scigility.anglerfish.data.avro

import scala.collection.immutable.{ ListMap, ListSet }
import scalaz.Applicative
import scalaz.Traverse
import AvroData._
import scalaz._
import Scalaz._
import shapeless.Typeable
import scala.collection.JavaConverters._

object implicits {



  implicit val avroTypeTraverse:Traverse[AvroType] = new Traverse[AvroType] {
    override def traverseImpl[G[_]:Applicative,A,B](fa:AvroType[A])(f:A => G[B]):G[AvroType[B]] = {
      val applicativeG = Applicative[G]
      def inner(at:AvroType[A]):G[AvroType[B]] = at match {
        case _:AvroNullType[A] => applicativeG.pure(AvroNullType[B])
        case _:AvroBooleanType[A] => applicativeG.pure(AvroBooleanType[B])
        case _:AvroIntType[A] => applicativeG.pure(AvroIntType[B])
        case _:AvroLongType[A] => applicativeG.pure(AvroLongType[B])
        case _:AvroFloatType[A] => applicativeG.pure(AvroFloatType[B])
        case _:AvroDoubleType[A] => applicativeG.pure(AvroDoubleType[B])
        case _:AvroBytesType[A] => applicativeG.pure(AvroBytesType[B])
        case _:AvroStringType[A] => applicativeG.pure(AvroStringType[B])
        case rec:AvroRecordType[A] => {
          val newFields:G[ListMap[AvroRecordFieldMetaData, B]] = Traverse[ListMap[AvroRecordFieldMetaData, ?]].traverse(rec.fields)(f)
          applicativeG.map(newFields)(flds => rec.copy[B](fields = flds))
        }
        case enum:AvroEnumType[A] => applicativeG.pure(enum.copy[B]())
        case AvroArrayType(items) => applicativeG.map(f(items))(AvroArrayType[B](_))
        case AvroMapType(values) => applicativeG.map(f(values))(AvroMapType[B](_))
        case AvroUnionType(members) => Traverse[List].traverse(members)(f).map(AvroUnionType[B](_))
        case fixed:AvroFixedType[A] => applicativeG.pure(fixed.copy[B]())
        case rec:AvroRecursionType[A] => applicativeG.map(f(rec.lazyType))(x => AvroRecursionType[B](rec.fqn, x))
      }
      inner(fa)
    }
  }


  implicit def avroValueTraverse[S]:Traverse[AvroValue[S, ?]] = new Traverse[AvroValue[S, ?]] {
    override def traverseImpl[G[_]:Applicative,A,B](fa:AvroValue[S, A])(f: A => G[B]): G[AvroValue[S,B]] = { 
      val applicativeG = Applicative[G]
      fa match {
        case AvroNullValue(s) => applicativeG.pure(AvroNullValue[S, B](s))
        case AvroBooleanValue(s, b) => applicativeG.pure(AvroBooleanValue[S, B](s, b))
        case AvroIntValue(s, i) =>  applicativeG.pure(AvroIntValue[S, B](s, i))
        case AvroLongValue(s, l) => applicativeG.pure(AvroLongValue[S, B](s, l))
        case AvroFloatValue(s, f) => applicativeG.pure(AvroFloatValue[S, B](s, f))
        case AvroDoubleValue(s, d) => applicativeG.pure(AvroDoubleValue[S, B](s, d))
        case AvroBytesValue(s, bytes) => applicativeG.pure(AvroBytesValue[S, B](s, bytes))
        case AvroStringValue(s, value) => applicativeG.pure(AvroStringValue[S, B](s, value))
        case AvroRecordValue(s, fields) => applicativeG.map(Traverse[ListMap[String, ?]].traverse(fields)(f))(flds => AvroRecordValue[S, B](s, flds))
        case AvroEnumValue(s, symbol) => applicativeG.pure(AvroEnumValue[S, B](s, symbol))
        case AvroArrayValue(s, items) =>  applicativeG.map(Traverse[List].traverse(items)(f))(itms => AvroArrayValue[S, B](s, itms)) 
        case AvroMapValue(s, values) =>   applicativeG.map(Traverse[Map[String, ?]].traverse(values)(f))(vals => AvroMapValue[S, B](s, vals))
        case AvroUnionValue(s, member) => applicativeG.map(f(member))(AvroUnionValue[S, B](s, _))
        case AvroFixedValue(s, bytes) => applicativeG.pure(AvroFixedValue[S, B](s, bytes))
      }
    }
  }


  implicit def listMapTraverse[K]:Traverse[ListMap[K, ?]] = new Traverse[ListMap[K, ?]] {

    override def traverseImpl[G[_]:Applicative,A,B](fa: ListMap[K, A])(f: A => G[B]): G[ListMap[K, B]] = {
      val applicativeG = Applicative[G]
      fa.foldLeft(applicativeG.pure(ListMap.empty[K,B]))(
        (map, elem) => {
          val mappedElem = f(elem._2)
          val mappedValue = applicativeG.map(mappedElem)( x => elem._1 -> x)
          applicativeG.apply2(map, mappedValue)(_ + _)
        }
      )
    }

  }


  implicit val listSetTraverse:Traverse[ListSet] = new Traverse[ListSet] {
    override def traverseImpl[G[_]:Applicative,A,B](fa: ListSet[A])(f: A => G[B]): G[ListSet[B]] = {
      val applicativeG = Applicative[G]
      fa.foldLeft(applicativeG.pure(ListSet.empty[B]))(
        (ls, elem) => {
          applicativeG.apply2(ls, f(elem))(_ + _)
        }
      )
    }
  }


  implicit val hashMapStringAnyTypeable:Typeable[java.util.HashMap[String,Any]] = new Typeable[java.util.HashMap[String,Any]] {
    def cast(t: Any):Option[java.util.HashMap[String,Any]] = t match {
      case null => None
      case refined:java.util.HashMap[_, _] => {
        if (refined.keySet().asScala.forall( _.isInstanceOf[String] )) Some(refined.asInstanceOf[java.util.HashMap[String,Any]]) else None
      }
      case _ => None
    }

    def describe:String = "java.util.HashMap[String, Any]"
  }

  implicit val genericDataArrayAnyTypeable:Typeable[org.apache.avro.generic.GenericData.Array[Any]] = new Typeable[org.apache.avro.generic.GenericData.Array[Any]] {
    def cast(t: Any):Option[org.apache.avro.generic.GenericData.Array[Any]] = t match {
      case null => None
      case refined:org.apache.avro.generic.GenericData.Array[_] => Some(refined.asInstanceOf[org.apache.avro.generic.GenericData.Array[Any]])
      case _ => None
    }

    def describe:String = "org.apache.avro.generic.GernicData.Array[Any]"
  }

}
