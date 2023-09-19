package com.socrata.pg.store

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.secondary.{DatasetInfo, Event, Secondary, VersionInfo}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}


class PlaybackBase extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase{
  override def beforeAll: Unit = {
    createDatabases()
  }

  def executePlayback(struct: Seq[(DatasetInfo, Int, Seq[Event[SoQLType, SoQLValue]], () => Unit)]) = {
    val secondary = new PGSecondary(config)
    struct.foldLeft(Option.empty[Secondary.Cookie]) { (initialCookie, struct) =>
      val (dataset, version, evs, fun) = struct
      val newCookie = Some(secondary.version(
                             new VersionInfo[SoQLType, SoQLValue] {
                               override val datasetInfo = dataset
                               override val initialDataVersion = version
                               override val finalDataVersion = version
                               override val cookie = initialCookie.orNull
                               override val events = evs.iterator
                               override val createdOrUpdatedRollups = Nil
                             }
                           ))
      fun()
      newCookie
    }
    secondary.shutdown()
  }

  def getCopyInfoByInternalDatasetName(internalDatasetName: String): Option[TruthCopyInfo] = {
    withPguUnconstrained() { pgu =>
      pgu.datasetMapReader.datasetIdForInternalName(internalDatasetName) match {
        case Some(id) =>
          pgu.datasetMapReader.datasetInfo(id) match {
            case Some(info) =>
              Some(pgu.datasetMapReader.latest(info))
            case _ => None
          }
        case _ => None
      }
    }
  }

  def getRollupByInternalDatasetNameAndName(internalDatasetName: String, rollupName: String): Option[LocalRollupInfo] = {
    withPguUnconstrained() { pgu =>
      getCopyInfoByInternalDatasetName(internalDatasetName) match {
        case Some(copy) =>
          pgu.datasetMapReader.rollup(copy, new RollupName(rollupName))
        case _ => None
      }
    }
  }

  def getSingleNumericValueFromStatement(statement: String): Option[java.math.BigDecimal] = {
    withPguUnconstrained() { pgu =>
      using(pgu.conn.prepareStatement(statement)) { stmt =>
        using(stmt.executeQuery()) { rs =>
          if (rs.next()) {
            Some(rs.getBigDecimal(1))
          } else {
            None
          }
        }
      }
    }
  }

}
