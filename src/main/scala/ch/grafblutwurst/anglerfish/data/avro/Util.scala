package ch.grafblutwurst.anglerfish.data.avro

import ch.grafblutwurst.anglerfish.data.avro.AvroData.{AvroName, AvroNamespace, AvroValidName, AvroValidNamespace}
import eu.timepit.refined.api.{Refined, Validate}
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string._
import eu.timepit.refined._
import matryoshka.data.Nu

object Util {

  //FIXME: ewww find out how to do proving for that so we can throw out the right.get
  def constructFQN(namespace:Option[AvroNamespace], name:AvroName):AvroNamespace =
    namespace.map(ns => refineV[AvroValidNamespace](ns.value + "." + name.value).right.get).getOrElse(refineV[AvroValidNamespace](name.value).right.get)

}
