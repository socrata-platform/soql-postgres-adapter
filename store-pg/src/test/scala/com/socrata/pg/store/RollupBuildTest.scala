package com.socrata.pg.store

import com.socrata.datacoordinator.id.{ColumnId, CopyId, RowId, UserColumnId}
import com.socrata.datacoordinator.secondary.{ColumnCreated, ColumnInfo, CopyInfo, DatasetInfo, Insert, LifecycleStage, RollupCreatedOrUpdated, RollupInfo, RowDataUpdated, Truncated, WorkingCopyCreated, WorkingCopyPublished}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil.{localeName, obfuscationKey, testInternalName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLID, SoQLNumber, SoQLText, SoQLVersion}
import org.joda.time.DateTime

class RollupBuildTest extends PlaybackBase {

  test("rollup rebuilds with unique table name when referenced dataset is updated") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, "_people5")
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, "_action5")
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action5 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

    var originalRollupInfo:Option[LocalRollupInfo] = None

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
          // store original rollupinfo
          originalRollupInfo = getRollupByInternalDatasetNameAndName(peopleDatasetInfo.internalName, peopleLikeCountRollup.name)

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
          val rollupInfo = getRollupByInternalDatasetNameAndName(peopleDatasetInfo.internalName, peopleLikeCountRollup.name).getOrElse(fail(s"Could not find rollup ${peopleLikeCountRollup.name} for ${peopleDatasetInfo.internalName}"))
          val original = originalRollupInfo.getOrElse(fail(s"Could not find rollup ${peopleLikeCountRollup.name} for ${peopleDatasetInfo.internalName}"))
          println(s"old table name: '${original.tableName}', new table name: '${rollupInfo.tableName}'")
          // updated rollupinfo should be using a different table name
          rollupInfo.tableName should not equal (original.tableName)
        }: Unit),
    )

    executePlayback(playback)

  }

}
