package com.socrata.pg.store

import java.util.Properties
import java.io.{FileOutputStream, File, FileInputStream}
import com.rojoma.simplearm.util._


/**
 * Store and Retrieve Dataset Metadata
 */
case class DatasetMeta(datasetInternalName: String, datasetSystemId: Long)

object DatasetMeta {

  def internalToFile(datasetInternalName: String) = {
    new File(datasetInternalName + ".properties")
  }

  def getMetadata(datasetInternalName: String): Option[DatasetMeta] = {
    val prop = new Properties()
    val propFile = internalToFile(datasetInternalName)
    if (propFile.exists()) {
      using(new FileInputStream(propFile)) {
        fis: FileInputStream => prop.load(fis)
      }
      Some(DatasetMeta(datasetInternalName,
        prop.getProperty("datasetSystemId", "-1").toLong))
    } else {
      None
    }
  }

  def setMetadata(md: DatasetMeta) {
    val prop = new Properties()
    val propFile = internalToFile(md.datasetInternalName)
    prop.setProperty("datasetSystemId", md.datasetSystemId.toString)
    using(new FileOutputStream(propFile)) {
      fos => prop.store(fos, "Temporary Mapping for Secondary Store Dataset IDs")
    }


  }

}
