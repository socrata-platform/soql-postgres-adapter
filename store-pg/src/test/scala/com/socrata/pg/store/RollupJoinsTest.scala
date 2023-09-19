package com.socrata.pg.store

import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil.{localeName, obfuscationKey, testInternalName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import org.joda.time.DateTime

class RollupJoinsTest extends PlaybackBase {

  test("rollup updates when referenced dataset is updated") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_people1"))
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_action1"))
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action1 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

    //Seq(dataset, version, Seq(Event), assertionFunction)
    val playback = Seq(
      (peopleDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLNumber, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("name"), Some(ColumnName("name")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, "adam"),
          (1, "john")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLNumber(BigDecimal.valueOf(r._1).bigDecimal))
            + (new ColumnId(3), new SoQLText(r._2))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (actionDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLID, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("person"), Some(ColumnName("person")), SoQLNumber, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(4), new UserColumnId("action"), Some(ColumnName("action")), SoQLText, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(5), new UserColumnId("subject"), Some(ColumnName("subject")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (peopleDatasetInfo, 2, Seq(
        RollupCreatedOrUpdated(peopleLikeCountRollup)),
        () => {
          //Adam should only have a single thing he likes (travel)
          val rollupInfo = getRollupByInternalDatasetNameAndName(peopleDatasetInfo.internalName, peopleLikeCountRollup.name).getOrElse(fail(s"Could not find rollup ${peopleLikeCountRollup.name} for ${peopleDatasetInfo.internalName}"))
          getSingleNumericValueFromStatement(s"select c3 from ${rollupInfo.tableName} where c2='adam'")
            .getOrElse(fail("Could not extract resultset")) should be(BigDecimal.valueOf(1))
        }: Unit),
      (actionDatasetInfo, 2, Seq(
        Truncated,
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance"),
          (2, 0, "like", "food")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        })),
        () => {
          //Adam initially only likes 1 thing (travel), then gets updated to like 2 things (travel,food)
          //If his like count is 2, that means the rollup was rebuilt
          val rollupInfo = getRollupByInternalDatasetNameAndName(peopleDatasetInfo.internalName, peopleLikeCountRollup.name).getOrElse(fail(s"Could not find rollup ${peopleLikeCountRollup.name} for ${peopleDatasetInfo.internalName}"))
          getSingleNumericValueFromStatement(s"select c3 from ${rollupInfo.tableName} where c2='adam'")
            .getOrElse(fail("Could not extract resultset")) should be(BigDecimal.valueOf(2))
        }: Unit),
    )

    executePlayback(playback)

  }

  test("dropping a rollup deletes the rollup_map and rollup_relationship_map records") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_people2"))
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_action2"))
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action2 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

    //Seq(dataset, version, Seq(Event), assertionFunction)
    val playback = Seq(
      (peopleDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLNumber, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("name"), Some(ColumnName("name")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, "adam"),
          (1, "john")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLNumber(BigDecimal.valueOf(r._1).bigDecimal))
            + (new ColumnId(3), new SoQLText(r._2))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (actionDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLID, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("person"), Some(ColumnName("person")), SoQLNumber, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(4), new UserColumnId("action"), Some(ColumnName("action")), SoQLText, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(5), new UserColumnId("subject"), Some(ColumnName("subject")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (peopleDatasetInfo, 2, Seq(
        RollupCreatedOrUpdated(peopleLikeCountRollup)),
        () => {
          withPguUnconstrained { pgu =>
            //We should have a single record in both these tables now
            val peopleCopy = getCopyInfoByInternalDatasetName(peopleDatasetInfo.internalName).getOrElse(fail(s"Could not find ${peopleDatasetInfo.internalName}"))
            val peopleRollup = pgu.datasetMapReader.rollup(peopleCopy, new RollupName(peopleLikeCountRollup.name))
            peopleRollup.isDefined should be(true)
            val actionCopy = getCopyInfoByInternalDatasetName(actionDatasetInfo.internalName).getOrElse(fail(s"Could not find ${actionDatasetInfo.internalName}"))
            pgu.datasetMapReader.getRollupCopiesRelatedToCopy(actionCopy) should have size (1)
          }
        }: Unit),
      (peopleDatasetInfo, 3, Seq(
        RollupDropped(peopleLikeCountRollup)),
        () => {
          withPguUnconstrained { pgu =>
            //We should have no records in both these tables now
            val peopleCopy = getCopyInfoByInternalDatasetName(peopleDatasetInfo.internalName).getOrElse(fail(s"Could not find ${peopleDatasetInfo.internalName}"))
            val peopleRollup = pgu.datasetMapReader.rollup(peopleCopy, new RollupName(peopleLikeCountRollup.name))
            peopleRollup.isDefined should be(false)
            val actionCopy = getCopyInfoByInternalDatasetName(actionDatasetInfo.internalName).getOrElse(fail(s"Could not find ${actionDatasetInfo.internalName}"))
            pgu.datasetMapReader.getRollupCopiesRelatedToCopy(actionCopy) should have size (0)
          }
        }: Unit),
    )

    executePlayback(playback)

  }

  test("dropping a dataset deletes the rollup_map and rollup_relationship_map records") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_people3"))
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_action3"))
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action3 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

    //Seq(dataset, version, Seq(Event), assertionFunction)
    val playback = Seq(
      (peopleDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLNumber, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("name"), Some(ColumnName("name")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, "adam"),
          (1, "john")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLNumber(BigDecimal.valueOf(r._1).bigDecimal))
            + (new ColumnId(3), new SoQLText(r._2))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (actionDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLID, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("person"), Some(ColumnName("person")), SoQLNumber, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(4), new UserColumnId("action"), Some(ColumnName("action")), SoQLText, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(5), new UserColumnId("subject"), Some(ColumnName("subject")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (peopleDatasetInfo, 2, Seq(
        RollupCreatedOrUpdated(peopleLikeCountRollup)),
        () => {
          withPguUnconstrained { pgu =>
            //We should have a single record in both these tables now
            val peopleCopy = getCopyInfoByInternalDatasetName(peopleDatasetInfo.internalName).getOrElse(fail(s"Could not find ${peopleDatasetInfo.internalName}"))
            val peopleRollup = pgu.datasetMapReader.rollup(peopleCopy, new RollupName(peopleLikeCountRollup.name))
            peopleRollup.isDefined should be(true)
            val actionCopy = getCopyInfoByInternalDatasetName(actionDatasetInfo.internalName).getOrElse(fail(s"Could not find ${actionDatasetInfo.internalName}"))
            pgu.datasetMapReader.getRollupCopiesRelatedToCopy(actionCopy) should have size (1)
          }
        }: Unit)
    )

    executePlayback(playback)
    val secondary = new PGSecondary(config)
    //Get this before deleting so we have access to what the copy id would have been, for the future assert query
    val actionCopy = getCopyInfoByInternalDatasetName(actionDatasetInfo.internalName).getOrElse(fail(s"Could not find ${actionDatasetInfo.internalName}"))

    secondary.dropDataset(actionDatasetInfo.internalName, None)
    withPguUnconstrained { pgu =>
      val peopleCopy = getCopyInfoByInternalDatasetName(peopleDatasetInfo.internalName).getOrElse(fail(s"Could not find ${peopleDatasetInfo.internalName}"))
      val peopleRollup = pgu.datasetMapReader.rollup(peopleCopy, new RollupName(peopleLikeCountRollup.name))
      //The rollup which references the dropped dataset should itself be deleted
      peopleRollup.isDefined should be(false)
      //We should have deleted from rollup_relationship_map since it references the deleted copyInfo
      pgu.datasetMapReader.getRollupCopiesRelatedToCopy(actionCopy) should have size (0)
    }
    secondary.shutdown()

  }

  test("row changed preview event does not cause rollup metadata to get deleted") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_people4"))
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_action4"))
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action4 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

    //Seq(dataset, version, Seq(Event), assertionFunction)
    val playback = Seq(
      (peopleDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLNumber, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("name"), Some(ColumnName("name")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, "adam"),
          (1, "john")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLNumber(BigDecimal.valueOf(r._1).bigDecimal))
            + (new ColumnId(3), new SoQLText(r._2))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (actionDatasetInfo, 1, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, 0L, 0L, DateTime.now())),
        ColumnCreated(ColumnInfo(new ColumnId(1), new UserColumnId("id"), Some(ColumnName("id")), SoQLID, true, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(2), new UserColumnId("version"), Some(ColumnName("version")), SoQLVersion, false, false, true, None)),
        ColumnCreated(ColumnInfo(new ColumnId(3), new UserColumnId("person"), Some(ColumnName("person")), SoQLNumber, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(4), new UserColumnId("action"), Some(ColumnName("action")), SoQLText, false, false, false, None)),
        ColumnCreated(ColumnInfo(new ColumnId(5), new UserColumnId("subject"), Some(ColumnName("subject")), SoQLText, false, false, false, None)),
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        }),
        WorkingCopyPublished),
        () => {}: Unit),
      (peopleDatasetInfo, 2, Seq(
        RollupCreatedOrUpdated(peopleLikeCountRollup)),
        () => {}: Unit),
      (actionDatasetInfo, 2, Seq(
        RowsChangedPreview(3, 0, 0, true),
        Truncated,
        RowDataUpdated(Seq(
          (0, 0, "like", "travel"),
          (1, 1, "like", "dance"),
          (2, 0, "like", "food")).map { r =>
          Insert(new RowId(r._1), ColumnIdMap()
            + (new ColumnId(1), new SoQLID(r._1))
            + (new ColumnId(3), new SoQLNumber(BigDecimal.valueOf(r._2).bigDecimal))
            + (new ColumnId(4), new SoQLText(r._3))
            + (new ColumnId(5), new SoQLText(r._4))
          )
        })),
        () => {
          withPguUnconstrained { pgu =>
            val actionCopy = getCopyInfoByInternalDatasetName(actionDatasetInfo.internalName).getOrElse(fail(s"Could not find ${actionDatasetInfo.internalName}"))
            pgu.datasetMapReader.getRollupCopiesRelatedToCopy(actionCopy) should have size (1)
          }
        }: Unit),
    )

    executePlayback(playback)
    
  }
}
