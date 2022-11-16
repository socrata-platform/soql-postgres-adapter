package com.socrata

object Predicates {

  type Predicate[T] = Function[T, Boolean]

  implicit class PredicateOperators[T](me: (T)=>Boolean) {

    def &(them: Predicate[T]): Predicate[T] = (t: T) => me(t) & them(t)

    def &&(them: Predicate[T]): Predicate[T] = (t: T) => me(t) && them(t)

    def |(them: Predicate[T]): Predicate[T] = (t: T) => me(t) | them(t)

    def ||(them: Predicate[T]): Predicate[T] = (t: T) => me(t) || them(t)

    // this allows you to specify negation of a predicate before its applied
    // e.g (!isAlphaNumeric && !isUnderscore)('a')
    val unary_! = (c: T) => !me(c)

  }

  def isAlphaNumericUnderscore: Predicate[Char] = isAlphaNumeric || isUnderscore

  def isAlphaNumeric: Predicate[Char] = Character.isLetterOrDigit

  def isUnderscore: Predicate[Char] = _ == '_'

}
