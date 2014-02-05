package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{ColumnInfo, DatasetInfo}
import com.socrata.soql.types.SoQLType
import java.sql.Connection

/**
 * Handles SystemRowIdentifierChangedEvent
 */
case class SystemRowIdentifierChangedHandler(secDatasetInfo: DatasetInfo, dataVersion: Long, secColumnInfo: ColumnInfo[SoQLType], conn:Connection) {

}
