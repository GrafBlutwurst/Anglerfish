package ch.grafblutwurst.anglerfish.data.avro

import ch.grafblutwurst.anglerfish.data.avro.AvroData.AvroType
import matryoshka.data.Nu
import org.scalacheck.Properties
import org.scalacheck.Prop._
import scalaz._
import Scalaz._
import matryoshka.implicits._
import ch.grafblutwurst.anglerfish.data.avro.implicits._

object TestJavaImplementationCongruence extends Properties("Java org.apache.avro congruence") {

  type M[A] = Either[Throwable, A]

  /*property("arbitrary schema fold -> unfold identity") = forAll {
    (schema: Nu[AvroType]) =>
      AvroGenericInteropFAlgebras.avroTypeToGenericSchema[M](schema).flatMap(avroSchema => avroSchema.anaM(AvroGenericInteropFAlgebras.avroSchemaToInternalType[M]))
      .map(unfolded => unfolded == schema)
      .fold(
        throwable => false :| throwable.getMessage(),
        result => result :| "Was same?"
      )

  }*/

}
