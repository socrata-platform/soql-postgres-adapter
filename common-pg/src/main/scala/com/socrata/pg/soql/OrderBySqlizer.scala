package com.socrata.pg.soql

import com.socrata.soql.typed.OrderBy
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}


class OrderBySqlizer(orderBy: OrderBy[UserColumnId, SoQLType]) extends Sqlizer[OrderBy[UserColumnId, SoQLType]] {

  import Sqlizer._

  val underlying = orderBy

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context, escape: Escape) = {
    val ParametricSql(s, setParamsOrderBy) = orderBy.expression.sql(rep, setParams, ctx, escape)
    val se = s + (if (orderBy.ascending) "" else " desc") + (if (orderBy.nullLast) " nulls last" else "")
    ParametricSql(se, setParamsOrderBy)
  }
}

