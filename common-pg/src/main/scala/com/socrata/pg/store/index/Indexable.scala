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

trait JsonbIndexable[T] extends Indexable[T] { this: SqlColumnCommonRep[T] =>

  override def createIndex(tableName: String, tablespace: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      DO $$$$ BEGIN
        IF NOT EXISTS(select 1 from pg_indexes WHERE indexname = 'idx_${tableName}_$phyCol') THEN
          CREATE INDEX idx_${tableName}_$phyCol on $tableName USING GIN ($phyCol)$tablespace;
        END IF;
      END; $$$$;"""
    }.mkString(";")
    Some(sql)
  }

  override def dropIndex(tableName: String): Option[String] = {
    val sql = this.physColumns.map { phyCol =>
      s"""
      DROP INDEX IF EXISTS idx_${tableName}_$phyCol"""
    }.mkString(";")
    Some(sql)
  }
}

trait NumberLikeIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait TimestampLikeIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait BooleanIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait BlobIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

trait PhotoIndexable[T] extends BaseIndexable[T] { this: SqlColumnCommonRep[T] => }

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
  private val sqlRepFactories = Map[SoQLType, (String, Seq[Int]) => SqlColIdx] (
    SoQLID -> ((base, _) => new IDRep(base)  with NoIndex[SoQLType]), // Already indexed
    SoQLVersion -> ((base, _) => new VersionRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLText -> ((base, _) => new TextRep(base) with TextIndexable[SoQLType]),
    SoQLBoolean -> ((base, _) => new BooleanRep(base) with BooleanIndexable[SoQLType]),
    SoQLNumber -> ((base, _) =>
      new NumberLikeRep(
        SoQLNumber,
        _.asInstanceOf[SoQLNumber].value,
        SoQLNumber(_),
        base) with NumberLikeIndexable[SoQLType]),
    SoQLMoney -> ((base, _) =>
      new NumberLikeRep(
        SoQLMoney,
        _.asInstanceOf[SoQLMoney].value,
        SoQLMoney(_),
        base) with NumberLikeIndexable[SoQLType]),
    SoQLFixedTimestamp -> ((base, _) => new FixedTimestampRep(base) with TimestampLikeIndexable[SoQLType]),
    SoQLFloatingTimestamp -> ((base, _) => new FloatingTimestampRep(base) with TimestampLikeIndexable[SoQLType]),
    SoQLDate -> ((base, _) => new DateRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLTime -> ((base, _) => new TimeRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLDouble -> ((base, _) => new DoubleRep(base) with NumberLikeIndexable[SoQLType]),
    SoQLObject -> ((base, _) => new ObjectRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLArray -> ((base, _) => new ArrayRep(base) with NoIndex[SoQLType]), // TODO: Revisit index need
    SoQLBlob -> ((base, _) => new BlobRep(base) with BlobIndexable[SoQLType]), // TODO: Revisit index need
    SoQLPhone -> ((base, _) => new PhoneRep(base) with TextIndexable[SoQLType]),
    SoQLLocation -> ((base, _) => new LocationRep(base) with LocationIndexable[SoQLType]),
    SoQLUrl -> ((base, _) => new UrlRep(base) with TextIndexable[SoQLType]),
    // document->filename @> 'literal'::jsonb so that this index can be used.
    // document->>filename = 'literal' will not use this index
    SoQLDocument -> ((base, _) => new DocumentRep(base) with JsonbIndexable[SoQLType]),
    SoQLPhoto -> ((base, _) => new PhotoRep(base) with BlobIndexable[SoQLType]),
    SoQLPoint -> ((base, levels) =>
      new GeometryLikeRep[Point](
        SoQLPoint,
        _.asInstanceOf[SoQLPoint].value,
        SoQLPoint(_),
        levels,
        base) with GeoIndexable[SoQLType]),
    SoQLMultiPoint -> ((base, levels) =>
      new GeometryLikeRep[MultiPoint](
        SoQLMultiPoint,
        _.asInstanceOf[SoQLMultiPoint].value,
        SoQLMultiPoint(_),
        levels,
        base) with GeoIndexable[SoQLType]),
    SoQLLine -> ((base, levels) =>
      new GeometryLikeRep[LineString](
        SoQLLine,
        _.asInstanceOf[SoQLLine].value,
        SoQLLine(_),
        levels,
        base) with GeoIndexable[SoQLType]),
    SoQLMultiLine -> ((base, levels) =>
      new GeometryLikeRep[MultiLineString](
        SoQLMultiLine,
        _.asInstanceOf[SoQLMultiLine].value,
        SoQLMultiLine(_),
        levels,
        base) with GeoIndexable[SoQLType]),
    SoQLPolygon -> ((base, levels) =>
      new GeometryLikeRep[Polygon](
        SoQLPolygon,
        _.asInstanceOf[SoQLPolygon].value,
        SoQLPolygon(_),
        levels,
        base) with GeoIndexable[SoQLType]),
    SoQLMultiPolygon -> ((base, levels) =>
      new GeometryLikeRep[MultiPolygon](
        SoQLMultiPolygon,
        _.asInstanceOf[SoQLMultiPolygon].value,
        SoQLMultiPolygon(_),
        levels,
        base) with GeoIndexable[SoQLType])
  )

  def sqlRep(columnInfo: ColumnInfo[SoQLType]): SqlColIdx =
    sqlRepFactories(columnInfo.typ)(columnInfo.physicalColumnBase, columnInfo.presimplifiedZoomLevels)
  def sqlRep(typ: SoQLType, baseName: String): SqlColIdx =
    sqlRepFactories(typ)(baseName, Seq.empty)
}
