package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId

case class QualifiedUserColumnId(val qualifier: Option[String], val userColumnId: UserColumnId)
