package ch.grafblutwurst.anglerfish.core.scalaZExtensions

import scalaz.MonadError

import scala.util.{Failure, Success, Try}

object MonadErrorSyntax {

  implicit class MonadErrorFromEither[M[_],  E ](M:MonadError[M, E]){
    def fromEither[A](e:Either[E, A]) = e.fold[M[A]](
      t => M.raiseError(t),
      a => M.pure(a)
    )
  }


  implicit class MonadErrorThrowable[M[_] ](M:MonadError[M, Throwable]){
    def fromTry[A](mayError: => Try[A]):M[A] = mayError match {
      case Success(value) => M.pure(value)
      case Failure(t) => M.raiseError[A](t)
    }

    def tryThunk[A](mayError: => A):M[A] = fromTry( Try {mayError})
  }

}
