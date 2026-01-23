package com.socrata.pg.store

import scala.annotation.tailrec
import scala.collection.{mutable => scm}

import com.rojoma.json.v3.ast.JNull
import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.json.v3.util.{AllowMissing, AutomaticJsonDecodeBuilder, JsonUtil}
import com.typesafe.scalalogging.Logger

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions, SoQLFunctionInfo, SoQLTypeInfo2}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.stdlib.analyzer2.{UserParameters, SoQLRewritePassHelpers}
import com.socrata.datacoordinator.truth.metadata.CopyInfo

import com.socrata.pg.analyzer2.metatypes.{RollupMetaTypes, DatabaseMetaTypes, DatabaseNamesMetaTypes}
import com.socrata.pg.analyzer2.{CryptProvidersByDatabaseNamesProvenance, CryptProviderProvider, RewriteSubcolumns}

final abstract class RollupAnalyzer

object RollupAnalyzer {
  private val logger = Logger[RollupAnalyzer]

  case class NewRollupSoqlInfo(
    foundTables: UnparsedFoundTables[RollupMetaTypes],
    @AllowMissing("Map.empty")
    locationSubcolumns: Map[
      types.DatabaseTableName[RollupMetaTypes],
      Map[types.DatabaseColumnName[RollupMetaTypes], Seq[Either[JNull, types.DatabaseColumnName[RollupMetaTypes]]]]
    ],
    @AllowMissing("Nil")
    rewritePasses: Seq[Seq[rewrite.AnyPass]],
    @AllowMissing("UserParameters.empty")
    userParameters: UserParameters
  )

  object NewRollupSoqlInfo {
    implicit val decode = AutomaticJsonDecodeBuilder[NewRollupSoqlInfo]
  }

  private val analyzer2 = locally {
    val standardSystemColumns = Set(":id", ":version", ":created_at", ":updated_at").map(ColumnName)

    new SoQLAnalyzer[RollupMetaTypes](new SoQLTypeInfo2, SoQLFunctionInfo, RollupMetaTypes.provenanceMapper).
      preserveSystemColumns {
        case (cname, expr) if standardSystemColumns(cname) =>
          Some(
            AggregateFunctionCall[RollupMetaTypes](
              MonomorphicFunction(SoQLFunctions.Max, Map("a" -> expr.typ)),
              Seq(expr),
              false,
              None
            )(FuncallPositionInfo.Synthetic)
          )
        case _ =>
          None
      }
  }

  type LocationSubcolumnsMap = Map[
    types.DatabaseTableName[DatabaseNamesMetaTypes],
    Map[
      types.DatabaseColumnName[DatabaseNamesMetaTypes],
      Seq[Option[types.DatabaseColumnName[DatabaseNamesMetaTypes]]]
    ]
  ]

  def apply(
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    currentCopy: CopyInfo,
    rollupInfo: LocalRollupInfo
  ): Option[(FoundTables[RollupMetaTypes], SoQLAnalysis[DatabaseNamesMetaTypes], LocationSubcolumnsMap, CryptProviderProvider)] =
  {
    val NewRollupSoqlInfo(foundTablesRaw, locationSubcolumnsRaw, rewritePasses, userParameters) = try {
      JsonUtil.parseJson[NewRollupSoqlInfo](rollupInfo.soql) match {
        case Right(si) => si
        case Left(err) =>
          logger.info("Invalid soql info in rollup? {}", err.english)
          return None
      }
    } catch {
      case e: JsonReaderException =>
        logger.info("Invalid json in rollup?", e)
        return None
    }

    val locationSubcolumns = locationSubcolumnsRaw.iterator.map { case (dtn, submap) =>
      dtn -> submap.iterator.map { case (dcn, cols) =>
        dcn -> cols.map {
          case Left(JNull) => None
          case Right(dcn2) => Some(dcn2)
        }
      }.toMap
    }.toMap

    val copyCache = new RollupMetaTypes.CopyCache(pgu, currentCopy)

    // ok, now we refresh the datasets with their current schemata
    val refinder = new FoundTablesTableFinder(foundTablesRaw) {
      override def foundDataset(name: ScopedResourceName, ds: Dataset) = {
        copyCache(ds.databaseName) match {
          case Some((copyInfo, columns)) =>
            val existingNames = new scm.HashSet[ColumnName]
            existingNames ++= ds.schema.valuesIterator.map(_.name)
            var nameCounter = 0

            @tailrec
            def freshFieldName(): ColumnName = {
              nameCounter += 1
              val candidate = ColumnName(s"c$nameCounter")
              if(existingNames(candidate)) {
                freshFieldName()
              } else {
                candidate
              }
            }

            val newSchema = OrderedMap() ++ columns.map { case (ucid, ci) =>
              val dcn = DatabaseColumnName(ucid)
              val oldColumn = ds.schema.get(dcn)
              // There are a couple of choices to make here in the
              // presence of changes.  The first is whether a
              // column that didn't exist before should be
              // considered "hidden" or not.  There's no exactly
              // right answer, alas, and in particular this
              // interacts with the meaning of `distinct` in the
              // face of `select *`.  But the worst case is that
              // we don't use a rollup when we could if we'd made
              // the other choice, and this is a pretty thin edge
              // case.
              val newHidden = oldColumn.map(_.hidden).getOrElse(false)

              // The other choice is to do with field name
              // mappings.  What I've chosen is to preserve the
              // field names of columns that existed when this
              // rollup was saved, and to name all other columns
              // with fresh names.  Rollup-matching will ignore
              // these names anyway.
              val newFieldName = oldColumn.map(_.name).getOrElse {
                val newName = freshFieldName()
                logger.debug("Column {} didn't exist before; rather than using its maybe-name {} we'll use {}", ucid, ci.fieldName, newName)
                newName
              }

              dcn -> DatasetColumnInfo(
                newFieldName,
                ci.typ,
                newHidden,
                None // we don't care about hints in rollup tables
              )
            }

            // We could be a little smarter about these (e.g., if the
            // user-defined PK has changed) but for now we'll just
            // preserve the old values to the extent that we can.
            val newOrdering =
              ds.ordering.filter { ordering =>
                newSchema.contains(ordering.column)
              }
            val newPrimaryKeys =
              ds.primaryKeys.filter { primaryKey =>
                primaryKey.forall(newSchema.contains(_))
              }

            Right(
              Dataset(
                ds.databaseName,
                ds.canonicalName,
                newSchema,
                newOrdering,
                newPrimaryKeys
              )
            )

          case None =>
            Left(LookupError.NotFound)
        }
      }
    }
    val foundTables = refinder.refind match {
      case Right(ft) =>
        ft
      case Left(err) =>
        logger.info("Failed to refind tables? " + err)
        return None
    }

    val baseAnalysis = analyzer2(foundTables, userParameters.toUserParameters) match {
      case Right(analysis) =>
        analysis
      case Left(err) => {
        logger.info("Failed to analyze rollup? {}", err)
        return None
      }
    }

    val dmtState = new DatabaseMetaTypes

    val metadataAnalysis = dmtState.rewriteFrom(baseAnalysis, copyCache, RollupMetaTypes.provenanceMapper) match {
      case Right(analysis) =>
        analysis
      case Left(missingTables) =>
        logger.info("Missing tables while converting rollups: {}", missingTables)
        return None
    }
    val cryptProviders = CryptProvidersByDatabaseNamesProvenance(metadataAnalysis.statement)

    val analysis = locally {
      val rewritePassHelpers = new SoQLRewritePassHelpers[DatabaseNamesMetaTypes]
      rewritePasses.foldLeft(
        DatabaseNamesMetaTypes.rewriteFrom(dmtState, metadataAnalysis)
      ) { (analysis, pass) =>
        analysis.applyPasses(pass, rewritePassHelpers)
      }
    }

    Some((foundTables, analysis, RewriteSubcolumns(locationSubcolumns, copyCache), cryptProviders))
  }
}
