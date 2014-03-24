package com.socrata.pg.store.index

import com.socrata.datacoordinator.common.soql.sqlreps._
import com.socrata.datacoordinator.truth.sql.{SqlColumnRep, SqlColumnCommonRep}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.soql.types._

trait Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  def createIndex(tableName: String, tablespace: String): Option[String]

  def dropIndex(tableName: String): Option[String]

}

trait NoIndex[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  def createIndex(tableName: String, tablespace: String): Option[String] = None

  def dropIndex(tableName: String): Option[String] = None

}

trait TextIndexable[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      // nl for order by col asc nulls lasts and col desc nulls first
      // nf for order by col asc nulls first and col desc nulls last
      // text_pattern_ops for like 'prefix%'
      s"""
      CREATE INDEX idx_${tableName}_$phyCol on $tableName USING BTREE ($phyCol text_pattern_ops)$tablespace;
      CREATE INDEX idx_${tableName}_u_$phyCol on $tableName USING BTREE (upper($phyCol) text_pattern_ops)$tablespace;
      CREATE INDEX idx_${tableName}_nl_$phyCol on $tableName USING BTREE ($phyCol nulls last)$tablespace;
      CREATE INDEX idx_${tableName}_nf_$phyCol on $tableName USING BTREE ($phyCol nulls first)$tablespace;
      CREATE INDEX idx_${tableName}_unl_$phyCol on $tableName USING BTREE (upper($phyCol) nulls last)$tablespace;
      CREATE INDEX idx_${tableName}_unf_$phyCol on $tableName USING BTREE (upper($phyCol) nulls first)$tablespace"""
    }.mkString(";")
    Some(sql)
  }

  def dropIndex(tableName: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      DROP INDEX IF EXISTS idx_${tableName}_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_u_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_nl_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_nf_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_unl_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_unf_$phyCol"""
    }.mkString(";")
    Some(sql)
  }
}

trait BaseIndexable[Type] extends Indexable[Type] { this: SqlColumnCommonRep[Type] =>

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      CREATE INDEX idx_${tableName}_nl_$phyCol on $tableName USING BTREE ($phyCol nulls last)$tablespace;
      CREATE INDEX idx_${tableName}_nf_$phyCol on $tableName USING BTREE ($phyCol nulls first)$tablespace"""
    }.mkString(";")
    Some(sql)
  }

  def dropIndex(tableName: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      DROP INDEX IF EXISTS idx_${tableName}_nl_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_nf_$phyCol"""
    }.mkString(";")
    Some(sql)
  }
}

trait NumberLikeIndexable[Type] extends BaseIndexable[Type] { this: SqlColumnCommonRep[Type] => }

trait TimestampLikeIndexable[Type] extends BaseIndexable[Type] { this: SqlColumnCommonRep[Type] => }

trait BooleanIndexable[Type] extends BaseIndexable[Type] { this: SqlColumnCommonRep[Type] => }


object SoQLIndexableRep {

  private val sqlRepFactories = Map[SoQLType, ColumnInfo[SoQLType] => SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]](
    SoQLID -> (ci => new IDRep(ci.physicalColumnBase)  with NoIndex[SoQLType]), // Already indexed
    SoQLVersion -> (ci => new VersionRep(ci.physicalColumnBase) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLText -> (ci => new TextRep(ci.physicalColumnBase) with TextIndexable[SoQLType]),
    SoQLBoolean -> (ci => new BooleanRep(ci.physicalColumnBase) with BooleanIndexable[SoQLType]),
    SoQLNumber -> (ci => new NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLNumber].value, SoQLNumber(_), ci.physicalColumnBase) with NumberLikeIndexable[SoQLType]),
    SoQLMoney -> (ci => new NumberLikeRep(SoQLNumber, _.asInstanceOf[SoQLMoney].value, SoQLMoney(_), ci.physicalColumnBase) with NumberLikeIndexable[SoQLType]),
    SoQLFixedTimestamp -> (ci => new FixedTimestampRep(ci.physicalColumnBase) with TimestampLikeIndexable[SoQLType]),
    SoQLFloatingTimestamp -> (ci => new FloatingTimestampRep(ci.physicalColumnBase) with TimestampLikeIndexable[SoQLType]),
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