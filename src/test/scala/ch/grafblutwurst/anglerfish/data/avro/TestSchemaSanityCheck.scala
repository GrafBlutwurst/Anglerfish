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
    } yield datum

     datum match {
      case Left(err) => {println(err.toString);err.printStackTrace(); false :| "could not decode schema"}
      case Right(value) => {
        println(value)
        true :| "decode worked"
      }
    }

  }




}
