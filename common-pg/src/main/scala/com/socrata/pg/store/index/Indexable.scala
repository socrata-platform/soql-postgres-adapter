package com.socrata.pg.store.index

import com.socrata.datacoordinator.common.soql.sqlreps._
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, SqlColumnCommonRep}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.soql.types._

trait Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  def createIndex(tableName: String): Option[String]

}

trait NoIndex[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  def createIndex(tableName: String): Option[String] = None

}

trait TextIndexable[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  override def createIndex(tableName: String) : Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"CREATE INDEX idx_${tableName}_$phyCol on $tableName USING BTREE ($phyCol text_pattern_ops);" +
        s"CREATE INDEX idx_${tableName}_u_$phyCol on $tableName USING BTREE (upper($phyCol) text_pattern_ops)"
    }.mkString(";")
    Some(sql)
  }

}

trait NumberLikeIndexable[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  override def createIndex(tableName: String) : Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"CREATE INDEX idx_${tableName}_n_$phyCol on $tableName USING BTREE ($phyCol)"
    }.mkString(";")
    Some(sql)
  }

}

object SoQLIndexableRep {

  private val sqlRepFactories = Map[SoQLType, ColumnInfo[SoQLType] => SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]](
    SoQLID -> (ci => new IDRep(ci.physicalColumnBase)  with NoIndex[SoQLType] ), // TODO: Revisit index need
    SoQLVersion -> (ci => new VersionRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLText -> (ci => new TextRep(ci.physicalColumnBase) with TextIndexable[SoQLType] ),
    SoQLBoolean -> (ci => new BooleanRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLNumber -> (ci => new NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_), ci.physicalColumnBase) with NumberLikeIndexable[SoQLType]),
    SoQLMoney -> (ci => new NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_), ci.physicalColumnBase) with NumberLikeIndexable[SoQLType]),
    SoQLFixedTimestamp -> (ci => new FixedTimestampRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLFloatingTimestamp -> (ci => new FloatingTimestampRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLDate -> (ci => new DateRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLTime -> (ci => new TimeRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLLocation -> (ci => new LocationRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLDouble -> (ci => new DoubleRep(ci.physicalColumnBase) with NumberLikeIndexable[SoQLType]),
    SoQLObject -> (ci => new ObjectRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLArray -> (ci => new ArrayRep(ci.physicalColumnBase) with NoIndex[SoQLType]) // TODO: Revisit index need
  )

  def sqlRep(columnInfo: ColumnInfo[SoQLType]): SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType] =
    sqlRepFactories(columnInfo.typ)(columnInfo)
}