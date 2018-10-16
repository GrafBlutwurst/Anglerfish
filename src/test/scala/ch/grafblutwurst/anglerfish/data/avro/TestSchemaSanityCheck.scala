package ch.grafblutwurst.anglerfish.data.avro

import scalaz._
import Scalaz._
import ch.grafblutwurst.anglerfish.data.avro.AvroData.AvroRecursionType
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.Properties
import matryoshka._
import matryoshka.implicits._
import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.avro.AvroJsonFAlgebras.UnexpectedTypeError
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import matryoshka.data.{Fix, Nu}

import scala.io.Source

object TestSchemaSanityCheck extends Properties("Sanity Check") {



  property("Recursive Schema Check") = {

    val schemaString =
      """
        |{
        |    "name":"foo",
        |    "type":"record",
        |     "fields":[
        |           {"name":"value", "type":"int"},
        |           {"name":"tail", "type":["null", "foo"], "default":null}
        |      ]
        |}
      """.stripMargin

    val avroJsonString =
      """
        |{
        | "value":1,
        | "tail": {
        |   "foo": {
        |     "value": 2,
        |     "tail": {
        |       "foo": {
        |         "value": 3,
        |         "tail": {
        |           "foo": {
        |             "value":4,
        |             "tail":{
        |                "foo" : {
        |                  "value":5,
        |                  "tail": {
        |                    "foo" : {
        |                      "value":6
        |                    }
        |                  }
        |                }
        |              }
        |             }
        |           }
        |         }
        |       }
        |     }
        |   }
        |
        |}
      """.stripMargin


    val datum = for {
      schemaNu <- AvroJsonFAlgebras.parseSchema[Either[Throwable, ?]](schemaString)
      datum <- AvroJsonFAlgebras.parseDatum[Either[Throwable, ?]](schemaNu)(avroJsonString)
      genFolded <- AvroGenericInteropFAlgebras.avroTypeToGenericSchema(schemaNu)
    } yield datum

     datum match {
      case Left(err) => {println(err.toString); false :| "could not decode schema"}
      case Right(value) => {
        println(value)
        true :| "decode worked"
      }
    }

  }


  property("testy") = {
    val schemaString = Source.fromFile("/home/grafblutwurst/Documents/schema.avsc").getLines().mkString("")
    val data = Source.fromFile("/home/grafblutwurst/Documents/avroJson.txt").getLines().toList


    val datum = for {
      schemaNu <- AvroJsonFAlgebras.parseSchema[Either[Throwable, ?]](schemaString).left.map(x => (schemaString, x))
      datum <- Traverse[List].traverse(data)( avroJsonString => AvroJsonFAlgebras.parseDatum[Either[Throwable, ?]](schemaNu)(avroJsonString).left.map(x => (avroJsonString, x)))
    } yield datum

    datum match {
      case Left((src, err@UnexpectedTypeError(base, context))) => {
        println(src)
        println(base)
        println(context)
        println(err.toString)
        err.printStackTrace()
        false :| "could not decode schema"
      }
      case Left(tpl) => {
        println(tpl._1)
        println(tpl._2.toString)
        tpl._2.printStackTrace()
        false :| "could not decode schema"
      }
      case Right(value) => {
        //println(value)
        true :| "decode worked"
      }
    }

  }

}
