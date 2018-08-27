package com.scigility.anglerfish.core.scalaZExtensions

import scalaz.MonadError

object MonadErrorSyntax {

  implicit class MonadErrorFromEither[M[_],  E ](M:MonadError[M, E]){
    def fromEither[A](e:Either[E, A]) = e.fold[M[A]](
      t => M.raiseError(t),
      a => M.pure(a)
    )
  }

}
