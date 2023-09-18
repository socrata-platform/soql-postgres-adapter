package com.socrata.db

  sealed trait SqlizerType
  case object Redshift extends SqlizerType
  case object Postgres extends SqlizerType


object SqlizerType {
  def parse: String => Option[SqlizerType] = {
    case "redshift" => Some(Redshift)
    case "postgres" => Some(Postgres)
    case _ => None
  }
}
