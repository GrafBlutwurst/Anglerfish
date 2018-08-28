package ch.grafblutwurst.anglerfish.data.avro

import scalaz._
import Scalaz._
import ch.grafblutwurst.anglerfish.data.avro.AvroData.AvroRecursionType
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.Properties
import matryoshka._
import matryoshka.implicits._
import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import matryoshka.data.{Fix, Nu}


object TestSchemaSanityCheck extends Properties("Sanity Check") {

  property("Recursive Schema Check") = {

    val schemaString =
      """
        |{
        |    "name":"foo",
        |    "type":"record",
        |     "fields":[
        |           {"name":"value", "type":"int"},
        |           {"name":"tail", "type":["null", "foo"]}
        |      ]
        |}
      """.stripMargin

    val avroJsonString =
      """
        |{
        | "value":0,
        | "tail": {
        |   "foo": {
        |     "value": 0,
        |     "tail": {
        |       "foo": {
        |         "value": 0,
        |         "tail": {
        |           "foo": {
        |             "value":15,
        |             "tail":null
        |           }
        |         }
        |       }
        |     }
        |   }
        | }
        |}
      """.stripMargin


    val datum = for {
      schemaNu <- AvroJsonFAlgebras.parseSchema[Either[Throwable, ?]](schemaString)
      datum <- AvroJsonFAlgebras.parseDatum[Either[Throwable, ?], Fix](schemaNu)(avroJsonString)
    } yield datum

     datum match {
      case Left(err) => {err.printStackTrace(); false :| "could not decode schema"}
      case Right(value) => {
        println(value)
        true :| "decode worked"
      }
    }

  }

}
