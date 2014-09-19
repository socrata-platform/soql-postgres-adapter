package com.socrata.pg.store

import com.socrata.datacoordinator.id.{DatasetId, UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.secondary.{DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo => TruthDatasetInfo}
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.soql.types._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import org.apache.log4j.PropertyConfigurator
import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.joda.time.DateTime

abstract class PGSecondaryTestBase extends FunSuite with Matchers with BeforeAndAfterAll with DatabaseTestBase {

  override def beforeAll = {
    PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))
    createDatabases()
  }

  def workingCopyCreatedFixture =
    new {
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, new DateTime())
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo)
      )
    }

  def columnsCreatedFixture =
    new {
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, new DateTime())
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), SoQLVersion, false, false, true)),
        ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), SoQLText, false, false, false)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, true, false, false))
      )
    }

  def columnsRemovedFixture =
    new {
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, new DateTime())
      val pgs = new PGSecondary(config)
      val testColInfo = ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), SoQLText, false, false, false)
      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), SoQLVersion, false, false, true)),
        ColumnCreated(testColInfo),
        ColumnRemoved(testColInfo),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, true, false, false))
      )
    }

  def getTruthDatasetInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], secondaryDatasetInfo: SecondaryDatasetInfo): TruthDatasetInfo = {
    val datasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
      throw new ResyncSecondaryException(s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}")
    )
    pgu.datasetMapReader.datasetInfo(datasetId).get
  }

  def getTruthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], secondaryDatasetInfo: SecondaryDatasetInfo): TruthCopyInfo = {
    pgu.datasetMapReader.latest(getTruthDatasetInfo(pgu, secondaryDatasetInfo))
  }

  def getTruthCopies(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], secondaryDatasetInfo: SecondaryDatasetInfo): Iterable[TruthCopyInfo] = {
    pgu.datasetMapReader.allCopies(getTruthDatasetInfo(pgu, secondaryDatasetInfo))
  }

  def dropDataset(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthDatasetId: DatasetId) = {
    val secondary = new PGSecondary(config)
    val dsName = s"$dcInstance.${truthDatasetId.underlying}"
    secondary.dropDataset(dsName, None)
    pgu.commit()
  }

  def cleanupDroppedtable(pgu: PGSecondaryUniverse[SoQLType, SoQLValue]) = {
    while (pgu.tableCleanup.cleanupPendingDrops()) { }
    pgu.commit()
  }
}
