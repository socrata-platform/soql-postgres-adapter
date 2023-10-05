package com.socrata.pg.store

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

import java.io.Closeable
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.socrata.curator.CuratorFromConfig
import com.socrata.pg.BuildInfo
import com.mchange.v2.c3p0.DataSources
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{RollupName, DatasetId, DatasetInternalName}
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo, CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetCopyContext, DatasetMapWriter, DatasetMapReader}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo => TruthDatasetInfo, CopyInfo => TruthCopyInfo, IndexInfo => TruthIndexInfo, LifecycleStage => TruthLifecycleStage}
import com.socrata.datacoordinator.truth.universe.sql.{C3P0WrappedPostgresCopyIn, PostgresCopyIn}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.SecondaryBase
import com.socrata.pg.config.StoreConfig
import com.socrata.pg.store.events.{ColumnCreatedHandler, WorkingCopyPublishedHandler, _}
import com.socrata.pg.store.index._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.postgresql.ds.PGSimpleDataSource

object PGSecondary {
  private val logger = Logger[PGSecondary]
}

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val storeConfig: StoreConfig) extends Secondary[SoQLType, SoQLValue] with SecondaryBase {
  import PGSecondary.logger

  logger.info(BuildInfo.toJson)

  override val dsConfig =  storeConfig.database

  private val dsInfo = DataSourceFromConfig.unmanaged(dsConfig)
  private val finished = new CountDownLatch(1)
  private val indexDropfinished = new CountDownLatch(1)
  private val tableDropper = startTableDropper()
  private val indexDropper = startIndexDropper()
  private val resyncBatchSize = storeConfig.resyncBatchSize
  private val tableDropTimeoutSeconds: Long = 60
  private val indexDropTimeoutSeconds: Long = 60
  private val curator = storeConfig.curatorConfig.map { cc =>
    val client = CuratorFromConfig.unmanaged(cc)
    client.start()
    client
  }

  val rowsChangedPreviewConfig =
    new RowsChangedPreviewConfig.Clojure(storeConfig.transparentCopyFunction.getOrElse(RowsChangedPreviewConfig.default))
  private val rowsChangedPreviewHandler = new RowsChangedPreviewHandler(rowsChangedPreviewConfig)

  val postgresUniverseCommon = new PostgresUniverseCommon(TablespaceFunction(storeConfig.tablespace), dsInfo.copyIn)

  private val secondaryMetrics = storeConfig.secondaryMetrics

  // Called when this process is shutting down (or being killed)
  def shutdown(): Unit = {
    logger.info("shutting down {} {} ...", dsConfig.host, dsConfig.database)
    finished.countDown()
    indexDropfinished.countDown()
    tableDropper.join()
    indexDropper.join()
    dsInfo.close()
    rowsChangedPreviewConfig match {
      case closable: Closeable => closable.close()
      case _ => // ok
    }
    curator.foreach(_.close())
    logger.info("shut down {} {}", dsConfig.host, dsConfig.database)
  }

  // This is the last event we will every receive for a given dataset. Receiving this event means
  // that all data related to that dataset can/should be destroyed
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie): Unit = {
    // last thing you will get for a dataset.
    logger.debug(s"dropDataset '${datasetInternalName}' (cookie : ${cookie}) ")
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) =>
          val datasetId = copyInfo.datasetInfo.systemId
          for { copy <- pgu.datasetMapReader.allCopies(copyInfo.datasetInfo) } {
            //Get all rollups that reference this copy
            pgu.datasetMapReader.getRollupsRelatedToCopy(copy).foreach{rollup=>
              //Schedule the built rollup table of the referenced copy for drop
              new RollupManager(pgu,rollup.copyInfo).dropRollup(rollup,false)
              //Drop the metadata here since the referenced dataset is not being dropped and this will not happen otherwise
              pgu.datasetMapWriter.dropRollup(rollup)
            }
            //Drop rollup_relationship metadata here since the official "drop routine" is in a library from data-coordinator, and DC does not keep track of rollup_relationships.
            //Future refactor would be ideal to keep deletion of rollup_map and rollup_relationship_map together.
            pgu.datasetMapReader.rollups(copy).foreach{rollup =>
              pgu.datasetMapWriter.deleteRollupRelationships(rollup)
            }
            //Schedule all built rollups for this copy to be dropped
            val rm = new RollupManager(pgu, copy)
            rm.dropRollups(immediate = false)
          }
          //Drop this dataset and eventually the rollup metadata
          pgu.datasetDropper.dropDataset(datasetId)
          pgu.commit()
        case None =>
          // TODO: Clean possible orphaned dataset internal name map
          logger.warn("drop dataset called but cannot find a copy {}", datasetInternalName)
      }
    }
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      doCurrentVersion(pgu, datasetInternalName, cookie)
    }
  }

  // Every set of changes increments the version number, so a given copy (number) may have
  // multiple versions over the course of it's life
  def doCurrentVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       datasetInternalName: String,
                       cookie: Secondary.Cookie): Long = {
    // every set of changes to a copy increments the version number
    // What happens when this is wrong? Almost certainly should turn into a resync
    logger.debug(s"currentVersion '${datasetInternalName}', (cookie: ${cookie})")

    truthCopyInfo(pgu, datasetInternalName) match {
      case Some(copy) => copy.dataVersion
      case None =>
        logger.warn("current version called but cannot find a copy {}", datasetInternalName)
        0
    }
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      doCurrentCopyNumber(pgu, datasetInternalName, cookie)
    }
  }

  // Current copy number is incremented every time a copy is made within the data coordinator
  // Publishing or snapshotting does not increment the copy number
  // The datasetmap contains both the current version number and current copy number and should be consulted to
  // determine the copy number value to return in this method. The datasetmap resides in the metadata db and can
  // be looked up by datasetInternalName
  protected[store] def doCurrentCopyNumber(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                           datasetInternalName: String,
                                           cookie: Secondary.Cookie): Long = {
    // Always incremented a working copy is made in the datacoordinator
    // the current copy number should always come out of the resync call or WorkingCopyCreatedEvent
    // still need the mapping from ds internal name to the copy number
    //
    // if we do not do working copies; we *should* receive a resync event instead of a publish event
    //
    // What happens if this is wrong? almost certainly it would turn into a resync
    logger.debug(s"currentCopyNumber '${datasetInternalName}' (cookie: ${cookie})")

    truthCopyInfo(pgu, datasetInternalName) match {
      case Some(copy) => copy.copyNumber
      case None =>
        logger.warn("current copy number called but cannot find a copy {}", datasetInternalName)
        0
    }
  }

  // Is only ever called as part of a resync.
  def dropCopy(datasetInfo: DatasetInfo,
               secondaryCopyInfo: SecondaryCopyInfo,
               cookie: Secondary.Cookie, isLatestCopy: Boolean): Secondary.Cookie = {
    logger.info("droping copy (datasetInfo: {}, secondaryCopyInfo: {}, cookie: {})",
                datasetInfo, secondaryCopyInfo, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      doDropCopy(pgu, datasetInfo, secondaryCopyInfo, isLatestCopy)
      pgu.commit()
    }
    cookie
  }

  def doDropCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: DatasetInfo,
                 secondaryCopyInfo: SecondaryCopyInfo, isLatestCopy: Boolean): Unit = {
    // If this is the latest copy we want make sure this copy is in the `copy_map`.
    // (Because secondary-watcher will write back to the secondary manifest that we
    // are up to this copy's data-version.)
    // If this is the case, resync(.) will have already been called on a (un)published
    // copy and appropriate metadata should exist for this dataset.
    // Otherwise, we want to drop this copy entirely.
    pgu.datasetMapReader.datasetIdForInternalName(datasetInfo.internalName) match {
      case None =>
        // Don't bother dropping a copy
        if (isLatestCopy) {
          // Well this is weird, but only going to log for now...
          logger.error("No dataset found for {} when dropping the latest copy {} for data version {}?",
            datasetInfo.internalName, secondaryCopyInfo.copyNumber.toString, secondaryCopyInfo.dataVersion.toString)
        }
      case Some(dsId) =>
        pgu.datasetMapReader.datasetInfo(dsId).map { truthDatasetInfo =>
          takeRollupLocks(pgu, truthDatasetInfo, Nil)
          pgu.datasetMapWriter.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber) match {
            case Some(copyInfo) =>
              // drop the copy
              logger.info(
                "dropping copy {} {}",
                truthDatasetInfo.systemId.toString,
                secondaryCopyInfo.copyNumber.toString
              )
              val rm = new RollupManager(pgu, copyInfo)
              rm.dropRollups(immediate = true) // drop all rollup tables
              pgu.datasetMapWriter.deleteCopy(copyInfo)
              if (isLatestCopy) {
                // create the `copy_map` entry
                pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, copyInfo.systemId, copyInfo.copyNumber,
                  TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                  secondaryCopyInfo.dataVersion,
                  secondaryCopyInfo.dataVersion)
              }
            case None =>
              // no copy to drop
              if (isLatestCopy) {
                // create the `copy_map` entry
                val secCopyId = pgu.datasetMapWriter.allocateCopyId()
                pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, secCopyId,
                  secondaryCopyInfo.copyNumber,
                  TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                  secondaryCopyInfo.dataVersion,
                  secondaryCopyInfo.dataVersion)
              }
          }
        }
    }
  }

  def version(versionInfo: VersionInfo[SoQLType, SoQLValue]): Secondary.Cookie = {
    withPgu(dsInfo, Some(versionInfo.datasetInfo)) { pgu =>
      val cookieOut = doVersion(pgu, versionInfo.datasetInfo, versionInfo.initialDataVersion, versionInfo.finalDataVersion, versionInfo.cookie, versionInfo.events, versionInfo.createdOrUpdatedRollups)
      pgu.commit()
      cookieOut
    }
  }

  def takeLocks(dmw: PGSecondaryDatasetMapWriter[SoQLType], dsIds: SortedSet[DatasetId]) = {
    for(dsId <- dsIds) {
      logger.debug("Locking updates against dataset {}", dsId)
      val timeout = new WarnAfter(30.seconds, logger, s"Locking $dsId took more than 30 seconds")
      try {
        timeout.start()
        if(dmw.datasetInfo(dsId, Duration.Inf).isEmpty) {
          logger.warn("Was told that dataset {} would be involved, but couldn't find its datasetInfo", dsId)
        }
      } finally {
        timeout.completed()
      }
    }
  }

  implicit object dsIdOrdering extends Ordering[DatasetId] {
    def compare(a: DatasetId, b: DatasetId) = a.underlying.compareTo(b.underlying)
  }

  type NameSet = Set[Either[String, DatasetInternalName]]

  def tableNamesToDatasetIds(dmr: PGSecondaryDatasetMapReader[SoQLType], tableNames: NameSet): SortedSet[DatasetId] = {
    SortedSet() ++ tableNames.iterator.flatMap {
      case Left(name) =>
        dmr.datasetInfoByResourceName(new ResourceName(name)) match {
          case Some(dsi) =>
            Some(dsi.systemId)
          case None =>
            logger.warn("Told I have a rollup that references table {} but I can't find it", JString(name))
            None
        }
      case Right(dsInternalName) =>
        dmr.datasetInfoByInternalName(dsInternalName) match {
          case Some(dsi) =>
            Some(dsi.systemId)
          case None =>
            logger.warn("Told I have a rollup that references table {} but I can't find it", dsInternalName)
            None
        }
    }
  }

  def relevantTableNames(rollup: RollupInfo): NameSet = {
    RollupManager.parseAndCollectTableNames(rollup)
  }

  def relevantTableNames(rollup: LocalRollupInfo): NameSet = {
    RollupManager.parseAndCollectTableNames(rollup)
  }

  // This is a little icky because rollups saved on other tables will
  // refer to their own table implicity (i.e., without a values
  // returned from relevantTableNames) so we'll track their dataset
  // ids directly.
  def existingRollupsTableNamesAndOrIds(dmr: PGSecondaryDatasetMapReader[SoQLType], dsInfo: TruthDatasetInfo): (NameSet, Set[DatasetId]) = {
    dmr.allCopies(dsInfo).foldLeft[(NameSet, Set[DatasetId])]((Set.empty, Set(dsInfo.systemId))) { case ((names, ids), copy) =>
      val newNames =
        dmr.rollups(copy).foldLeft(names) { (acc, rollup) =>
          acc ++ relevantTableNames(rollup)
        }
      dmr.getRollupsRelatedToCopy(copy).foldLeft((newNames, ids)) { case ((names, ids), rollup) =>
        (names ++ relevantTableNames(rollup), (ids + rollup.copyInfo.datasetInfo.systemId))
      }
    }
  }

  def newRollupsTableNames(rollups: Seq[RollupInfo]): NameSet = {
    rollups.foldLeft[NameSet](Set.empty) { (acc, rollup) =>
      acc ++ relevantTableNames(rollup)
    }
  }

  def takeRollupLocks(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], dsInfo: TruthDatasetInfo, upcomingRollups: Seq[RollupInfo]): Unit = {
    val (names, ids) = existingRollupsTableNamesAndOrIds(pgu.datasetMapReader, dsInfo)
    takeLocks(
      pgu.datasetMapWriter,
      tableNamesToDatasetIds(
        pgu.datasetMapReader,
        names ++ newRollupsTableNames(upcomingRollups)
      ) ++ ids
    )
  }

  // The main method by which data will be sent to this API.
  // workingCopyCreated event is the (first) event by which this method will be called
  // "You always update the latest copy through the version method"
  // A separate event will be passed to this method for actually copying the data
  // If working copy already exists and we receive a workingCopyCreated event is received, then a resync
  // event/exception should fire.
  // Publishing a working copy promotes that working copy to a published copy.
  // There should no longer be a working copy after publishing
  // scalastyle:ignore method.length cyclomatic.complexity
  def doVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                        secondaryDatasetInfo: DatasetInfo,
                        newDataVersion: Long,
                        finalDataVersion: Long,
                        cookie: Secondary.Cookie,
                        events: Iterator[Event[SoQLType, SoQLValue]],
                        upcomingRollups: Seq[RollupInfo]): Secondary.Cookie = {
    // How do we get the copyInfo? dataset_map
    //  - One of the events that comes through here will be working copy created, it must be the first if it does,
    //    separate event for actually copying the data
    //  - Always update the latest copy through version if no working copy created event is passed in
    //  - If you have a working copy and you get a working copy created event; resync
    //  - If you don't have a working copy and you get a publish event; resync
    //  - Unpublished => Working Copy

    // rowVersion is given through the event
    // newDataVersion is the version which corresponds to the set of events which we are given and
    //   corresponds with the currentVersion
    //     - ignore this if the newDataVersion <= currentVersion
    //     - stored in copy_map
    logger.debug("version (secondaryDatasetInfo: %s, newDataVersion: %s, cookie: %s, events: %s)".format(
      secondaryDatasetInfo, newDataVersion, cookie, events
      ))
    // if we have a WorkingCopyCreated event, it is supposed to be the first event in the version,
    // and the only WorkingCopyCreated event in the version.
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    val hasWccEvents = wccEvents.hasNext

    if (wccEvents.hasNext) {
      val e = wccEvents.next()
      logger.debug("got working copy event: {}", e)

      val existingDataset =
        for {
          datasetId <- pgu.datasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName)
          datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
        } yield {
          datasetInfo
        }

      e match {
        case WorkingCopyCreated(copyInfo) =>
          val theCopy = existingDataset match {
            case Some(dsInfo) =>
              takeRollupLocks(pgu, dsInfo, upcomingRollups)
              val allCopies = pgu.datasetMapReader.allCopies(dsInfo)
              // Either this copy or some newer copy
              allCopies.find(existingCopyInfo => existingCopyInfo.copyNumber >= copyInfo.copyNumber)
            case None =>
              None
          }
          if (theCopy.isDefined) {
            logger.info("dataset {} working copy {} already existed, resync",
              secondaryDatasetInfo.internalName, theCopy.get.copyNumber.toString)
            throw new ResyncSecondaryException("Dataset already exists")
          } else {
            WorkingCopyCreatedHandler(pgu,
              pgu.datasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName),
              secondaryDatasetInfo, copyInfo)
          }
        case _ => throw new UnsupportedOperationException("Unexpected operation")
      }
    }

    if (wccEvents.hasNext) throw new UnsupportedOperationException(
        s"Got ${wccEvents.size + 1} leading WorkingCopyCreated events, only support one in a version"
      )

    // now that we have working copy creation out of the way, we can load our
    // secondaryCopyInfo with assurance that it is there unless we are out of sync
    val dsId = pgu.datasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
      throw new ResyncSecondaryException(
        s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}"
      )
    )

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(dsId).get
    val initialTruthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

    // This is redundant (but harmless, because we have all the locks)
    // if the WorkingCopyCreated-on-an-existing-dataset path was taken
    // before.
    takeRollupLocks(pgu, truthDatasetInfo, upcomingRollups)

    // If there's more than one version's difference between the last known copy in truth
    // and the last known copy in the secondary, force a resync.
    // TODO : It seems weird to resync just because there's a version gap,
    // but the PG secondary code has always worked this way, and we've never
    // verified that playing back multiple versions in one go actually works in
    // every case. At some point, we should take the time to verify this and
    // revisit whether we actually want this resync logic to exist.
    val allCopiesInOrder = pgu.datasetMapReader.allCopies(truthDatasetInfo)
    val dvExpect = allCopiesInOrder.maxBy(_.dataVersion).dataVersion + 1
    if (newDataVersion != dvExpect) {
      throw new ResyncSecondaryException(
       s"Current version ${initialTruthCopyInfo.dataVersion}, next version ${newDataVersion} but should be ${dvExpect}")
    }

    if (!hasWccEvents) {
      // If there are no writes that have happened here yet,
      // commit to release unnecessary locks on system tables like dataset_internal_name_map, dataset_map.
      // rows updates still cause locks on copy_map, column_map though.
      pgu.commit()
    }

    sealed abstract class RollupUpdate {
      def increaseTo(that: RollupUpdate): RollupUpdate
    }
    case object TryToCreate extends RollupUpdate {
      // "(Re)create all rollups if we're in an appropriate publication cycle stage"
      def increaseTo(that: RollupUpdate) =
        that match {
          case ForceCreate => ForceCreate
          case _ => this
        }
    }
    case object DoNothing extends RollupUpdate { // for drop-working-copy
      // "Don't do anything at all with rollups"
      def increaseTo(that: RollupUpdate) = that
    }
    case class TryToMove(except: Set[RollupName]) extends RollupUpdate {
      // "Move any rollups from the previous version, or create if necessary, if we're in an appropriate publication cycle stage"
      def increaseTo(that: RollupUpdate) =
        that match {
          case ttm: TryToMove =>
            TryToMove(this.except ++ ttm.except)
          case _ =>
            that
        }
    }
    case object ForceCreate extends RollupUpdate { // for optimize
      // "Create all rollups even if we're in an inappropriate publication cycle stage"
      def increaseTo(that: RollupUpdate) = this
    }

    // no rebuild index and no rows loader.
    case class Ctx(
      rebuildIndex: Boolean,
      truthCopyInfo: TruthCopyInfo,
      dataLoader: Option[SqlPrevettedLoader[SoQLType, SoQLValue]],
      rowDataProgress: RowDataUpdatedHandler.Progress,
      rollupUpdate: RollupUpdate,
      indexesUpdate: Seq[TruthIndexInfo]
    ) {
      def withRollupUpdate(ru: RollupUpdate) = copy(rollupUpdate = rollupUpdate.increaseTo(ru))
    }
    val startCtx = Ctx(false, initialTruthCopyInfo, None, RowDataUpdatedHandler.Progress(), TryToMove(Set.empty), Seq.empty)
    val endCtx = remainingEvents.foldLeft(startCtx) { (ctx, e) =>
      logger.debug("got event: {}", e)
      e match {
        case Truncated =>
          TruncateHandler(pgu, ctx.truthCopyInfo)
          ctx.withRollupUpdate(TryToCreate)
        case ColumnCreated(secondaryColInfo) =>
          ColumnCreatedHandler(pgu, ctx.truthCopyInfo, secondaryColInfo)
          ctx.copy(dataLoader = None).withRollupUpdate(TryToCreate)
        case ColumnRemoved(secondaryColInfo) =>
          ColumnRemovedHandler(pgu, ctx.truthCopyInfo, secondaryColInfo)
          ctx.copy(dataLoader = None).withRollupUpdate(TryToCreate)
        case FieldNameUpdated(secondaryColInfo) =>
          FieldNameUpdatedHandler(pgu, ctx.truthCopyInfo, secondaryColInfo)
          ctx.copy(dataLoader = None).withRollupUpdate(TryToCreate)
        case RowIdentifierSet(info) =>  // no-op
          ctx
        case RowIdentifierCleared(info) =>  // no-op
          ctx
        case SystemRowIdentifierChanged(secondaryColInfo) =>
          SystemRowIdentifierChangedHandler(pgu, ctx.truthCopyInfo, secondaryColInfo)
          ctx
        case VersionColumnChanged(secondaryColInfo) =>
          VersionColumnChangedHandler(pgu, ctx.truthCopyInfo, secondaryColInfo)
          ctx
        // TODO when dealing with dropped, figure out what version we have to updated afterwards and how that works...
        // maybe workingcopydropped is guaranteed to be the last event in a batch, and then we can skip updating the
        // version anywhere... ?
        case WorkingCopyDropped =>
          WorkingCopyDroppedHandler(pgu, truthDatasetInfo)
          ctx.copy(dataLoader = None).withRollupUpdate(DoNothing)
        case DataCopied =>
          DataCopiedHandler(pgu, truthDatasetInfo, ctx.truthCopyInfo)
          ctx.withRollupUpdate(TryToCreate)
        case SnapshotDropped(info) =>
          logger.info("drop snapshot system id - {}, copy number - {}",
            info.systemId.toString(), info.copyNumber.toString)
          pgu.datasetMapReader.copyNumber(truthDatasetInfo, info.copyNumber) match {
            case Some(ci) =>
              CopyDroppedHandler(pgu, ci)
            case None =>
              throw new Exception("cannot find copy to drop")
          }
          ctx.copy(dataLoader = None)
        case WorkingCopyPublished =>
          val publishedCopyInfo = WorkingCopyPublishedHandler(pgu, ctx.truthCopyInfo)
          ctx.copy(rebuildIndex = true, truthCopyInfo = publishedCopyInfo).withRollupUpdate(TryToMove(except = Set.empty))
        case RowDataUpdated(ops) =>
          val loader = ctx.dataLoader.getOrElse(prevettedLoader(pgu, ctx.truthCopyInfo))
          val rdp = RowDataUpdatedHandler(loader, ops, ctx.rowDataProgress)
          ctx.copy(dataLoader = Some(loader), rowDataProgress = rdp).withRollupUpdate(TryToCreate)
        case LastModifiedChanged(lastModified) =>
          pgu.datasetMapWriter.updateLastModified(ctx.truthCopyInfo, lastModified)
          ctx
        case RollupCreatedOrUpdated(rollupInfo) =>
          RollupCreatedOrUpdatedHandler(pgu, ctx.truthCopyInfo, rollupInfo)
          ctx.withRollupUpdate(TryToMove(Set(new RollupName(rollupInfo.name)))) // why isn't this a RN already?

        case RollupDropped(rollupInfo) =>
          RollupDroppedHandler(pgu, ctx.truthCopyInfo, rollupInfo)
          ctx
        case IndexCreatedOrUpdated(indexInfo) =>
          val idx = IndexCreatedOrUpdatedHandler(pgu, ctx.truthCopyInfo, indexInfo)
          ctx.copy(indexesUpdate = ctx.indexesUpdate :+ idx)
        case IndexDropped(indexName) =>
          IndexDroppedHandler(pgu, ctx.truthCopyInfo, indexName)
          ctx
        case ComputationStrategyCreated(_) => // no op
          ctx
        case ComputationStrategyRemoved(_) => // no op
          ctx
        case RowsChangedPreview(inserted, updated, deleted, truncated) =>
          val updatedCtx =
            rowsChangedPreviewHandler(pgu, truthDatasetInfo, ctx.truthCopyInfo, truncated, inserted, updated, deleted) match {
              case Some(nci) =>
                val isPublished = nci.lifecycleStage == TruthLifecycleStage.Published
                ctx.copy(rebuildIndex = isPublished, truthCopyInfo = nci, dataLoader = None)
              case None =>
                ctx
            }
          val rdp = updatedCtx.rowDataProgress
          updatedCtx.copy(
            rowDataProgress = rdp.copy(expectedOps = rdp.expectedOps + inserted + updated + deleted)
          ).withRollupUpdate(TryToCreate)
        case SecondaryReindex =>
          ctx.copy(rebuildIndex = true).withRollupUpdate(ForceCreate)
        case IndexDirectiveCreatedOrUpdated(secondaryColInfo, directive) =>
          IndexDirectiveCreatedOrUpdatedHandler(pgu, truthColumnInfo(pgu, ctx.truthCopyInfo, secondaryColInfo), directive)
          ctx.copy(rebuildIndex = true)
        case IndexDirectiveDropped(secondaryColInfo) =>
          IndexDirectiveDroppedHandler(pgu, truthColumnInfo(pgu, ctx.truthCopyInfo, secondaryColInfo))
          ctx.copy(rebuildIndex = true)
        case otherOps: Event[SoQLType,SoQLValue] =>
          throw new UnsupportedOperationException(s"Unexpected operation $otherOps")
      }
    }

    pgu.datasetMapWriter.updateDataVersion(endCtx.truthCopyInfo, finalDataVersion, true)

    def maybeLogTime[T](label: String)(f: => T): T = {
      val startedAt =
        if(endCtx.truthCopyInfo.tableModifier != initialTruthCopyInfo.tableModifier) {
          Some(System.nanoTime())
        } else {
          None
        }
      val result = f
      startedAt.foreach { start =>
        val durationMS = (System.nanoTime() - start)/1000000
        logger.info(s"$label after making a copy took ${durationMS / 1000.0}s")
      }
      result
    }

    if (endCtx.rebuildIndex) {
      maybeLogTime("Rebuilding indexes") {
        val schema = pgu.datasetMapReader.schema(endCtx.truthCopyInfo)
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        val indexDirectives = pgu.datasetMapReader.indexDirectives(endCtx.truthCopyInfo, None)
        sLoader.createIndexes(schema.values)
        sLoader.createFullTextSearchIndex(schema.values, indexDirectives)
        pgu.analyzer.analyze(endCtx.truthCopyInfo)
      }
    }

    if(endCtx.rollupUpdate != DoNothing) {
      val postUpdateTruthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
      maybeLogTime("Refreshing rollups") {
        updateRollups(pgu,
                      Some(initialTruthCopyInfo),
                      postUpdateTruthCopyInfo,
                      tryToMove = { rollup =>
                        endCtx.rollupUpdate match {
                          case TryToMove(except) => !except.contains(rollup)
                          case _ => false
                        }
                      },
                      force = endCtx.rollupUpdate == ForceCreate)
        updateRelatedRollups(postUpdateTruthCopyInfo)
      }
    }

    def updateRelatedRollups(copyInfo: TruthCopyInfo): Unit ={
      pgu.datasetMapReader.getRollupCopiesRelatedToCopy(copyInfo).foreach(relatedCopy=>{
        logger.info(s"Updating rollups for ${relatedCopy.systemId}, as they relate to current ${copyInfo.systemId}")
        updateRollups(pgu,
          Some(relatedCopy),
          relatedCopy,
          tryToMove = _ =>false,
          force = false
        )
      })
    }

    val im = new IndexManager(pgu, endCtx.truthCopyInfo)
    if (im.justPublish(startCtx.truthCopyInfo) || endCtx.indexesUpdate.nonEmpty) {
      im.updateIndexes(endCtx.indexesUpdate)
    }
    cookie
  }

  // This is an expensive operation in that it is both time consuming as well as locking the data source for the
  // duration of the resync event. The resync event can come from either the DC, or originate from a
  // ResyncSecondaryException being thrown.
  // Incoming rows have their own ids already provided at this point
  // Need to record some state somewhere so that readers can know that a resync is underway
  // Backup code (Receiver class) touches this method as well via the receiveResync method
  // SoQL ID is only ever used for row ID
  def resync(datasetInfo: DatasetInfo,
             secondaryCopyInfo: SecondaryCopyInfo,
             schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
             cookie: Secondary.Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo],
             indexDirectives: Seq[com.socrata.datacoordinator.truth.metadata.IndexDirective[SoQLType]],
             indexes: Seq[IndexInfo],
             isLatestLivingCopy: Boolean): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.info("resync (datasetInfo: {}, secondaryCopyInfo: {}, schema: {}, cookie: {})",
      datasetInfo, secondaryCopyInfo, schema, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      val cookieOut = doResync(pgu, datasetInfo, secondaryCopyInfo, schema, cookie, rows, rollups, indexDirectives, indexes)
      pgu.commit()
      cookieOut
    }
  }

  // scalastyle:ignore method.length cyclomatic.complexity
  def doResync(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
               secondaryDatasetInfo: DatasetInfo,
               secondaryCopyInfo: SecondaryCopyInfo,
               newSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
               cookie: Secondary.Cookie,
               rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
               rollups: Seq[RollupInfo],
               indexDirectives: Seq[com.socrata.datacoordinator.truth.metadata.IndexDirective[SoQLType]],
               indexes: Seq[IndexInfo]): Secondary.Cookie =
  {
    val truthCopyInfo = pgu.datasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName) match { // scalastyle:ignore line.size.limit
      case None =>
        // The very top record - dataset_internal_name_map is missing.  Start everything new from new dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest
        //      SET broken_at = null,
        //          latest_secondary_data_version = 0,
        //          latest_secondary_lifecycle_stage = 'Unpublished'
        //    WHERE dataset_system_id = ? -- 20
        //   DELETE from falth.dataset_internal_name_map
        //    WHERE dataset_internal_name = ? -- 'alpha.20'
        logger.info("re-creating secondary dataset with new id")
        val newDatasetInfo = pgu.datasetMapWriter.unsafeCreateDatasetAllocatingSystemId(secondaryDatasetInfo.localeName, secondaryDatasetInfo.obfuscationKey, secondaryDatasetInfo.resourceName)
        takeRollupLocks(pgu, newDatasetInfo, rollups)
        val newDatasetId = newDatasetInfo.systemId
        logger.info("new secondary dataset {} {}", secondaryDatasetInfo.internalName, newDatasetId.toString())
        pgu.datasetMapWriter.createInternalNameMapping(secondaryDatasetInfo.internalName, newDatasetId)
        val secCopyId = pgu.datasetMapWriter.allocateCopyId()
        pgu.datasetMapWriter.unsafeCreateCopy(newDatasetInfo, secCopyId,
          secondaryCopyInfo.copyNumber,
          TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
          secondaryCopyInfo.dataVersion,
          secondaryCopyInfo.dataVersion)
      case Some(dsId) =>
        // Find the very top record - dataset_internal_name_map.
        // Delete and recreate the copy with the same dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest
        //      SET broken_at = null,
        //          latest_secondary_data_version = 0,
        //          latest_secondary_lifecycle_stage = 'Unpublished'
        //    WHERE dataset_system_id = ? -- 20
        // Disable dataset for soql reads during resync to avoid lock issue which
        // leads to backing up of connections waiting for lock.
        pgu.datasetMapWriter.disableDataset(dsId)
        pgu.commit()
        pgu.datasetMapReader.datasetInfo(dsId).map { truthDatasetInfo =>
          takeRollupLocks(pgu, truthDatasetInfo, rollups)
          pgu.datasetMapWriter.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber) match {
            case Some(copyInfo) =>
              logger.info(
                "delete existing copy so that a new one can be created with the same ids {} {}",
                truthDatasetInfo.systemId.toString(),
                secondaryCopyInfo.copyNumber.toString
              )
              val rm = new RollupManager(pgu, copyInfo)
              rm.dropRollups(immediate = true) // drop all rollup tables
              pgu.datasetMapWriter.deleteCopy(copyInfo)
              pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, copyInfo.systemId, copyInfo.copyNumber,
                TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                secondaryCopyInfo.dataVersion,
                secondaryCopyInfo.dataVersion)
            case None =>
              val secCopyId = pgu.datasetMapWriter.allocateCopyId()
              pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, secCopyId,
                secondaryCopyInfo.copyNumber,
                TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                secondaryCopyInfo.dataVersion,
                secondaryCopyInfo.dataVersion)
          }
        }.getOrElse(throw new Exception(
          s"""Cannot find existing dataset info.
             |  You may manually delete dataset_internal_name_map record and start fresh
             |  ${secondaryDatasetInfo.internalName} ${dsId}
           """.stripMargin))
    }

    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(truthCopyInfo)

    newSchema.values.foreach { col =>
      ColumnCreatedHandler(pgu, truthCopyInfo, col)
    }

    val truthSchema: ColumnIdMap[ColumnInfo[SoQLType]] = pgu.datasetMapReader.schema(truthCopyInfo)
    val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
    sLoader.deoptimize(truthSchema.values)
    val loader = pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "test-user"))

    val sidColumnInfo = newSchema.values.find(_.isSystemPrimaryKey).get

    for { iter <- rows } {
      // TODO: divide rows based on data size instead of number of rows.
      iter.grouped(resyncBatchSize).foreach { bi =>
        for { row: ColumnIdMap[SoQLValue] <- bi } {
           logger.trace("adding row: {}", row)
           val rowId = pgu.commonSupport.typeContext.makeSystemIdFromValue(row.get(sidColumnInfo.systemId).get)
           loader.insert(rowId, row)
        }
        loader.flush()
      }
    }
    truthSchema.values.find(_.isSystemPrimaryKey).foreach { pkCol =>
      sLoader.makeSystemPrimaryKey(pkCol)
    }
    sLoader.optimize(truthSchema.values, pgu.datasetMapReader.indexDirectives(truthCopyInfo, None))

    if (truthCopyInfo.lifecycleStage == TruthLifecycleStage.Unpublished &&
        secondaryCopyInfo.lifecycleStage == LifecycleStage.Published) {
      pgu.datasetMapWriter.publish(truthCopyInfo)
    }

    if (truthCopyInfo.dataVersion != secondaryCopyInfo.dataVersion) {
      pgu.datasetMapWriter.updateDataVersion(truthCopyInfo, secondaryCopyInfo.dataVersion, true)
    }
    pgu.datasetMapWriter.updateLastModified(truthCopyInfo, secondaryCopyInfo.lastModified)

    val postUpdateTruthCopyInfo = pgu.datasetMapReader.latest(truthCopyInfo.datasetInfo)
    // re-create rollup metadata
    for { rollup <- rollups } RollupCreatedOrUpdatedHandler(pgu, postUpdateTruthCopyInfo, rollup)

    // re-create rollup tables.  We don't need to pass in an old truth
    // because we just dropped any existing rollups above.
    updateRollups(pgu, None, postUpdateTruthCopyInfo, tryToMove = Function.const(false), force = false)

    // re-create index directives
    for { idx <- indexDirectives } {
      IndexDirectiveCreatedOrUpdatedHandler(pgu, idx.columnInfo, idx.directive)
    }

    // re-create indexes
    if (indexes.nonEmpty) {
      for { idx <- indexes } {
        IndexCreatedOrUpdatedHandler(pgu, postUpdateTruthCopyInfo, idx)
      }
      val im = new IndexManager(pgu, postUpdateTruthCopyInfo)
      im.updateIndexes()
    }

    pgu.datasetMapWriter.enableDataset(truthCopyInfo.datasetInfo.systemId) // re-enable soql reads
    cookie
  }

  private def truthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            datasetInternalName: String): Option[TruthCopyInfo] = {
    for {
      datasetId <- pgu.datasetMapReader.datasetIdForInternalName(datasetInternalName)
      truthDatasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      pgu.datasetMapReader.latest(truthDatasetInfo)
    }
  }

  private def truthColumnInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                              truthCopyInfo: TruthCopyInfo,
                              secColInfo: SecondaryColumnInfo[SoQLType]): ColumnInfo[SoQLType] = {

      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
      val truthCopyContext = new DatasetCopyContext[SoQLType](truthCopyInfo, pgu.datasetMapReader.schema(truthCopyInfo))
      truthCopyContext.thaw().columnInfo(secColInfo.systemId)
  }

  private def updateRollups(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            originalCopyInfo: Option[TruthCopyInfo],
                            copyInfo: TruthCopyInfo,
                            tryToMove: RollupName => Boolean,
                            force: Boolean) = {
    val rm = new RollupManager(pgu, copyInfo)
    pgu.datasetMapReader.rollups(copyInfo).foreach { ri =>
      rm.updateRollup(ri, originalCopyInfo, tryToMove, force)
    }
  }

  private def prevettedLoader(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo) = {
      val truthSchema = pgu.datasetMapReader.schema(truthCopyInfo)
      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
      pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "user"))
  }

  private def startTableDropper() = {
    val tableDropper = new Thread() {
      setName(s"pg-sec-table-dropper-${storeConfig.database.host}")
      override def run(): Unit = {
        while (!finished.await(tableDropTimeoutSeconds, TimeUnit.SECONDS)) {
          try {
            withPgu(dsInfo, None) { pgu =>
              while(finished.getCount > 0 && pgu.tableCleanup.cleanupPendingDrops()) {
                pgu.commit()
              }
            }
          } catch {
            case e: Exception => logger.error("Unexpected error while dropping secondary tables", e)
          }
        }
      }
    }
    tableDropper.start()
    tableDropper
  }

  private def startIndexDropper() = {
    val dropper = new Thread() {
      val host = storeConfig.database.host
      setName(s"pg-sec-index-dropper-${host}")
      override def run(): Unit = {
        while (!indexDropfinished.await(indexDropTimeoutSeconds, TimeUnit.SECONDS)) {
          try {
            withPgu(dsInfo, None) { pgu =>
              while(indexDropfinished.getCount > 0 && pgu.indexCleanup.cleanupPendingDrops(dsInfo.dataSource, host)) {
                logger.info("index drop sleep")
              }
            }
          } catch {
            case e: Exception => logger.error("Unexpected error while dropping indexes", e)
          }
        }
      }
    }
    dropper.start()
    dropper
  }

  override def metric(datasetInternalName: String, cookie: Cookie): Option[SecondaryMetric] = {
    if (secondaryMetrics.enabled) {
      logger.debug(s"metric '$datasetInternalName' (cookie: $cookie)")
      withPgu(dsInfo, None) { pgu =>
        val result = doMetric(pgu, datasetInternalName)
        pgu.commit()

        result
      }
    } else {
      logger.debug("skipping metric since dataset secondary metrics are not enabled")
      None
    }
  }

  private def doMetric(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       datasetInternalName: String): Option[SecondaryMetric] = {
    pgu.datasetMapReader.datasetIdForInternalName(datasetInternalName).flatMap { datasetId =>
      pgu.secondaryMetrics.dataset(datasetId)
    }
  }
}
