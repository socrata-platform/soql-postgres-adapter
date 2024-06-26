package com.socrata.pg.store

import com.socrata.datacoordinator.id.{DatasetId, UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.secondary.{DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo => TruthDatasetInfo}
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import org.apache.log4j.PropertyConfigurator
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.joda.time.DateTime

abstract class PGSecondaryTestBase extends FunSuite with Matchers with BeforeAndAfterAll with DatabaseTestBase {

  val totalRows = 18

  override def beforeAll(): Unit = {
    createDatabases()
  }

  def workingCopyCreatedFixture = new { // scalastyle:ignore
    val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, freshResourceNameRaw())
    val dataVersion = 0L
    val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
    val pgs = new PGSecondary(config)
    val events = Seq(
      WorkingCopyCreated(copyInfo)
    )
  }

  def columnsCreatedFixture = new { // scalastyle:ignore
    val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, freshResourceNameRaw())
    val dataVersion = 0L
    val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
    val pgs = new PGSecondary(config)
    val events = Seq(
      WorkingCopyCreated(copyInfo),
      ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), Some(ColumnName("my_column")), SoQLText, false, false, false, None)),
      SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None))
    )
  }

  def publishedDatasetFixture = new { // scalastyle:ignore
    val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, freshResourceNameRaw())
    val dataVersion = 0L
    val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
    val pgs = new PGSecondary(config)
    val events = Seq(
      WorkingCopyCreated(copyInfo),
      ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), Some(ColumnName("my_column")), SoQLText, false, false, false, None)),
      SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None)),
      WorkingCopyPublished
    )
  }

  def columnsRemovedFixture = new { // scalastyle:ignore
    val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, freshResourceNameRaw())
    val dataVersion = 0L
    val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
    val pgs = new PGSecondary(config)
    val testColInfo = ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), Some(ColumnName("my_column")), SoQLText, false, false, false, None)
    val events = Seq(
      WorkingCopyCreated(copyInfo),
      ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None)),
      ColumnCreated(testColInfo),
      ColumnRemoved(testColInfo),
      SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None))
    )
  }

  def fieldNameUpdatedFixture = new { // scalastyle:ignore
  val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, freshResourceNameRaw())
    val dataVersion = 0L
    val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
    val pgs = new PGSecondary(config)
    val testColInfo = ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), Some(ColumnName("my_column")), SoQLText, false, false, false, None)
    val events = Seq(
      WorkingCopyCreated(copyInfo),
      ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
      ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None)),
      ColumnCreated(testColInfo),
      FieldNameUpdated(testColInfo.copy(fieldName = Some(ColumnName("my_column_2")))),
      SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None))
    )
  }

  def getTruthDatasetInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                          secondaryDatasetInfo: SecondaryDatasetInfo): TruthDatasetInfo = {
    val datasetId = pgu.datasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
      throw new ResyncSecondaryException(s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}")
    )
    pgu.datasetMapReader.datasetInfo(datasetId).get
  }

  def getTruthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       secondaryDatasetInfo: SecondaryDatasetInfo): TruthCopyInfo =
    pgu.datasetMapReader.latest(getTruthDatasetInfo(pgu, secondaryDatasetInfo))

  def getTruthCopies(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                     secondaryDatasetInfo: SecondaryDatasetInfo): Iterable[TruthCopyInfo] =
    pgu.datasetMapReader.allCopies(getTruthDatasetInfo(pgu, secondaryDatasetInfo))

  def dropDataset(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthDatasetId: DatasetId): Unit = {
    val secondary = new PGSecondary(config)
    val dsName = s"$dcInstance.${truthDatasetId.underlying}"
    secondary.dropDataset(dsName, None)
    pgu.commit()
  }

  def cleanupDroppedTables(pgu: PGSecondaryUniverse[SoQLType, SoQLValue]): Unit = {
    while (pgu.tableCleanup.cleanupPendingDrops()) { }
    pgu.commit()
  }
}
