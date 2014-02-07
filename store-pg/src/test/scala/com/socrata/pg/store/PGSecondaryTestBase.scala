package com.socrata.pg.store

import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.secondary.{DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.soql.types._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.DatasetInfo

class PGSecondaryTestBase  extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll = {
    val rootConfig = ConfigFactory.load()
    PropertyConfigurator.configure(Propertizer("log4j", rootConfig.getConfig("com.socrata.soql-server-pg.log4j")))
  }

  def workingCopyCreatedFixture =
    new {
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo)
      )
    }

  def columnsCreatedFixture =
    new {
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = SecondaryCopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), SoQLVersion, false, false, true)),
        ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), SoQLText, false, false, false)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, true, false, false))
      )
    }

    def getTruthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], secondaryDatasetInfo: SecondaryDatasetInfo): TruthCopyInfo = {
      val datasetId = pgu.datasetInternalNameMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
        throw new ResyncSecondaryException(s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}")
      )

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
      pgu.datasetMapReader.latest(truthDatasetInfo)
    }

}
