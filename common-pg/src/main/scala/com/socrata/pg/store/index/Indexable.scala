package com.socrata.pg.store.index

import com.socrata.datacoordinator.common.soql.sqlreps._
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.pg.soql.SqlColIdx
import com.socrata.soql.types._
import com.vividsolutions.jts.geom.{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}

trait Indexable[T] { this: SqlColumnCommonRep[T] =>
  def createIndex(tableName: String, tablespace: String): Option[String]
  def createRollupIndex(tableName: String, tablespace: String): Option[String] = createIndex(tableName, tablespace)
  def dropIndex(tableName: String): Option[String]
}

trait NoIndex[T] extends Indexable[T] { this: SqlColumnCommonRep[T] =>
  def createIndex(tableName: String, tablespace: String): Option[String] = None
  def dropIndex(tableName: String): Option[String] = None
}

// scalastyle:off regex multiple.string.literals
trait TextIndexable[T] extends Indexable[T] { this: SqlColumnCommonRep[T] =>
  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      // nl for order by col asc nulls lasts and col desc nulls first and range scans
      // text_pattern_ops for like 'prefix%' or equality
      s"""DO $$$$ BEGIN
         |  IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_u_${phyCol}') THEN
         |    CREATE INDEX idx_${tableName}_u_${phyCol} on ${tableName}
         |    USING BTREE (upper(${phyCol}) text_pattern_ops)${tablespace};
         |  END IF;
         |  IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_nl_${phyCol}') THEN
         |    CREATE INDEX idx_${tableName}_nl_${phyCol} on ${tableName}
         |    USING BTREE (${phyCol} nulls last)${tablespace};
         |  END IF;
         |  IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_unl_${phyCol}') THEN
         |    CREATE INDEX idx_${tableName}_unl_${phyCol} on ${tableName}
         |    USING BTREE (upper(${phyCol}) nulls last)${tablespace};
         |  END IF;
         |END; $$$$;
       """.stripMargin
    }.mkString(";")
    Some(sql)
  }

  override def createRollupIndex(tableName: String, tablespace: String): Option[String] = {
    // In the bigger picture, the choice of indexes to create should be more dynamic based on
    // type of queries we expect to the table and/or dynamically determined from workload.
    // For now we are putting minimal indexes on rollup tables since their main use case is
    // making small tables we can efficiently do full table scans on.  We are not typically
    // doing prefix matching or case insensitive matching against them.

    val sql = this.physColumns.map { phyCol =>
      s"""
      DO $$$$ BEGIN
        IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_nl_$phyCol') THEN
          CREATE INDEX idx_${tableName}_nl_$phyCol on $tableName USING BTREE ($phyCol nulls last)$tablespace;
        END IF;
      END; $$$$;"""
    }.mkString(";")
    Some(sql)
  }

  override def dropIndex(tableName: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      // we used to create nulls first indexes, so drop those if exist
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

trait BaseIndexable[T] extends Indexable[T] { this: SqlColumnCommonRep[T] =>

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      DO $$$$ BEGIN
        IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_nl_$phyCol') THEN
          CREATE INDEX idx_${tableName}_nl_$phyCol on $tableName USING BTREE ($phyCol nulls last)$tablespace;
        END IF;
      END; $$$$;"""
    }.mkString(";")
    Some(sql)
  }

  override def dropIndex(tableName: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      // we used to create a nulls first index, so also drop it if exists
      s"""
      DROP INDEX IF EXISTS idx_${tableName}_nl_$phyCol;
      DROP INDEX IF EXISTS idx_${tableName}_nf_$phyCol"""
    }.mkString(";")
    Some(sql)
  }
}

trait RowIdentifierIndexable[T] extends Indexable[T] { this: SqlColumnCommonRep[T] =>

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      // This is temporary remedy to make up for missing primary key.  Primary key must not have more than one column.
      // index name should allow matching Data Coordinator primary key name and yet allow us to detect where it is created.
      val indexName = "uniq_" + tableName + "_" + phyCol
      s"""
      DO $$$$ BEGIN
        IF NOT EXISTS(select 1 from pg_indexes WHERE indexname like '${indexName}%') THEN
          CREATE UNIQUE INDEX ${indexName}_s on $tableName USING BTREE ($phyCol)$tablespace;
        END IF;
      END; $$$$;"""
    }.mkString(";")
    Some(sql)
  }

  override def dropIndex(tableName: String): Option[String] = None
}

trait NumberLikeIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait TimestampLikeIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait BooleanIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait BlobIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait GeoIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] =>

  def indexableColumns: Array[String] = this.physColumns

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = indexableColumns.map { phyCol =>
      s"""
      DO $$$$ BEGIN
        IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_${phyCol}_gist') THEN
          CREATE index idx_${tableName}_${phyCol}_gist ON ${tableName} USING GIST(${phyCol})$tablespace;
        END IF;
      END; $$$$;"""
    }.mkString(";")
    Some(sql)
  }

  override def dropIndex(tableName: String): Option[String] = {
    val sql = indexableColumns.map { phyCol =>
      s"""DROP INDEX IF EXISTS idx_${tableName}_${phyCol}_gist"""
    }.mkString(";")
    Some(sql)
  }
}

/**
 * TODO: Is there a need to create index for latitude, longitude and address fields?
 * @tparam T
 */
trait LocationIndexable[T] extends GeoIndexable[T] { this: SqlColumnCommonRep[T] =>

  override def indexableColumns: Array[String] = this.physColumns.take(1)
}


object SoQLIndexableRep {
  private val sqlRepFactories = Map[SoQLType, String => SqlColIdx] (
    // TODO: Change SoQLID back to NoIndex when the missing primary key problem is fixed
    SoQLID -> (base => new IDRep(base) with RowIdentifierIndexable[SoQLType]), // Already indexed
    SoQLVersion -> (base => new VersionRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLText -> (base => new TextRep(base) with TextIndexable[SoQLType]),
    SoQLBoolean -> (base => new BooleanRep(base) with BooleanIndexable[SoQLType]),
    SoQLNumber -> (base =>
      new NumberLikeRep(
        SoQLNumber,
        _.asInstanceOf[SoQLNumber].value,
        SoQLNumber(_),
        base) with NumberLikeIndexable[SoQLType]),
    SoQLMoney -> (base =>
      new NumberLikeRep(
        SoQLMoney,
        _.asInstanceOf[SoQLMoney].value,
        SoQLMoney(_),
        base) with NumberLikeIndexable[SoQLType]),
    SoQLFixedTimestamp -> (base => new FixedTimestampRep(base) with TimestampLikeIndexable[SoQLType]),
    SoQLFloatingTimestamp -> (base => new FloatingTimestampRep(base) with TimestampLikeIndexable[SoQLType]),
    SoQLDate -> (base => new DateRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLTime -> (base => new TimeRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLDouble -> (base => new DoubleRep(base) with NumberLikeIndexable[SoQLType]),
    SoQLObject -> (base => new ObjectRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLArray -> (base => new ArrayRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLBlob -> (base => new BlobRep(base) with BlobIndexable[SoQLType]), // TODO: Revisit index need
    SoQLPhone -> (base => new PhoneRep(base) with TextIndexable[SoQLType]),
    SoQLLocation -> (base => new LocationRep(base) with LocationIndexable[SoQLType]),
    SoQLUrl -> (base => new UrlRep(base) with TextIndexable[SoQLType]),
    SoQLPoint -> (base =>
      new GeometryLikeRep[Point](
        SoQLPoint,
        _.asInstanceOf[SoQLPoint].value,
        SoQLPoint(_),
        base) with GeoIndexable[SoQLType]),
    SoQLMultiPoint -> (base =>
      new GeometryLikeRep[MultiPoint](
        SoQLMultiPoint,
        _.asInstanceOf[SoQLMultiPoint].value,
        SoQLMultiPoint(_),
        base) with GeoIndexable[SoQLType]),
    SoQLLine -> (base =>
      new GeometryLikeRep[LineString](
        SoQLLine,
        _.asInstanceOf[SoQLLine].value,
        SoQLLine(_),
        base) with GeoIndexable[SoQLType]),
    SoQLMultiLine -> (base =>
      new GeometryLikeRep[MultiLineString](
        SoQLMultiLine,
        _.asInstanceOf[SoQLMultiLine].value,
        SoQLMultiLine(_),
        base) with GeoIndexable[SoQLType]),
    SoQLPolygon -> (base =>
      new GeometryLikeRep[Polygon](
        SoQLPolygon,
        _.asInstanceOf[SoQLPolygon].value,
        SoQLPolygon(_),
        base) with GeoIndexable[SoQLType]),
    SoQLMultiPolygon -> (base =>
      new GeometryLikeRep[MultiPolygon](
        SoQLMultiPolygon,
        _.asInstanceOf[SoQLMultiPolygon].value,
        SoQLMultiPolygon(_),
        base) with GeoIndexable[SoQLType])
  )

  def sqlRep(columnInfo: ColumnInfo[SoQLType]): SqlColIdx =
    sqlRepFactories(columnInfo.typ)(columnInfo.physicalColumnBase)
  def sqlRep(typ: SoQLType, baseName: String): SqlColIdx =
    sqlRepFactories(typ)(baseName)
}
