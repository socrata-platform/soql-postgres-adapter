package com.socrata.pg.server

import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PGSecondaryUniverse, PostgresUniverseCommon}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

class SoQLJoinTest extends SoQLTest {

  test("plain") {
    compareSoqlResult("select make, name, @manufacturer.timezone join @manufacturer on make=@manufacturer.make order by make, code",
                      "join.json",
                      joinDatasetCtx = plainCtx)

  }

  test("alias") {
    compareSoqlResult("select make, name, @m.timezone join @manufacturer as m on make=@m.make where @m.make='OZONE' order by @m.make, code",
                      "join-where.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join a table twice") {
    compareSoqlResult("select make, name, @m2.timezone join @manufacturer as m on make=@m.make join @manufacturer as m2 on make=@m2.make where @m.make='OZONE' order by @m.make, code",
      "join-where.json",
      joinDatasetCtx = aliasCtx)
  }

  test("chain count") {
    compareSoqlResult("select make, name, @m.timezone join @manufacturer as m on make=@m.make where @m.make='OZONE' |> select count(*)",
                      "join-chain-count.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("group and then join") {
    compareSoqlResult("select make, count(name) as ct group by make |> select make, ct, @m.timezone join @manufacturer as m on make=@m.make where @m.make='OZONE'",
                      "group-join.json",
                      joinDatasetCtx = aliasCtx)
  }
}
