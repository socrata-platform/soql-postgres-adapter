package com.socrata.pg.store

import com.socrata.datacoordinator.id.{ColumnId, CopyId, RowId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil.{localeName, obfuscationKey, testInternalName}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLID, SoQLNumber, SoQLText, SoQLVersion}
import org.joda.time.DateTime

class RollupCopyDroppedHandlerTest extends PlaybackBase {

  test("dropping an unpublished working copy handles rollup metadata deletion") {
    val peopleDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_people6"))
    val actionDatasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, Some("_action6"))
    val peopleLikeCountRollup = RollupInfo("peopleLikeCount", "SELECT _id, _name, count(@actions._subject) AS `like_count` JOIN @action6 AS @actions ON _id = @actions._person WHERE @actions._action='like' GROUP BY _id, _name")

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
        () => {}: Unit),
      (peopleDatasetInfo, 3, Seq(
        WorkingCopyCreated(CopyInfo(new CopyId(-1), 2, LifecycleStage.Unpublished, 0L, 0L, DateTime.now())),
        WorkingCopyDropped),
        () => {
            //no error happens
        }: Unit),
    )

    executePlayback(playback)

  }

}
