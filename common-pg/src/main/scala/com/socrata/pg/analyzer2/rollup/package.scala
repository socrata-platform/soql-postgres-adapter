package com.socrata.pg.analyzer2

package object rollup {
  implicit class OptionExt[T](private val ts: Option[T]) extends AnyVal {
    def mapFallibly[U](f: T => Option[U]): Option[Option[U]] =
      ts match {
        case None => Some(None)
        case Some(t) => f(t).map(Some(_))
      }
  }

  implicit class TraversableOnceExt[T](private val ts: TraversableOnce[T]) extends AnyVal {
    def mapFallibly[U](f: T => Option[U]): Option[Seq[U]] = {
      val result = Vector.newBuilder[U]
      for(t <- ts) {
        f(t) match {
          case Some(u) =>
            result += u
          case None =>
            return None
        }
      }
      Some(result.result())
    }
  }
}
