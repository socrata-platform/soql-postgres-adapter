package com.socrata.pg.analyzer2

package object iterutil {
  implicit class AugmentedIterator[T](private val underlying: Iterator[T]) extends AnyVal {
    def foldMap[S, U](initialValue: S)(f: (S, T) => (S, U)): Iterator[U] = {
      new Iterator[U] {
        var state = initialValue

        def hasNext = underlying.hasNext
        def next(): U = {
          val (newState, result) = f(state, underlying.next())
          state = newState
          result
        }
      }
    }
  }
}
