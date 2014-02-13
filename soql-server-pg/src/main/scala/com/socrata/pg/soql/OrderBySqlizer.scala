package com.socrata.pg.soql

import com.socrata.soql.typed.OrderBy
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.types.SoQLType

class OrderBySqlizer(orderBy: OrderBy[UserColumnId, SoQLType]) extends Sqlizer[OrderBy[UserColumnId, SoQLType]] {

  import Sqlizer._

  def sql = {
    orderBy.expression.sql + (if (orderBy.ascending) "" else " desc") + (if (orderBy.nullLast) " nulls last" else "")
  }
}

