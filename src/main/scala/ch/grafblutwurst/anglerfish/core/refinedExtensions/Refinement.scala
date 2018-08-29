package ch.grafblutwurst.anglerfish.core.refinedExtensions

import scalaz.Scalaz._
import scalaz._
import eu.timepit.refined._
import eu.timepit.refined.api.{Refined, Validate}
import ch.grafblutwurst.anglerfish.core.scalaZExtensions.MonadErrorSyntax._

object Refinement {

  def refineME[M[_], E, A, P](a:A)(ef: String => E)(implicit M:MonadError[M, E], ev:Validate[A, P]):M[A Refined P] =
    M.fromEither(refineV[P](a).leftMap(ef))

}
