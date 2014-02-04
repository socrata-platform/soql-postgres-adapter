package com.socrata.pg.store

import java.util.Properties
import java.io.{FileOutputStream, File, FileInputStream}
import com.rojoma.simplearm.util._


/**
 * Store and Retrieve Dataset Metadata
 */
case class DatasetMeta(datasetInternalName: String, copy: Long, version: Long, locale: String, obfuscationKey: String, primaryKey: String)

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
        prop.getProperty("copy", "0").toLong,
        prop.getProperty("version", "0").toLong,
        prop.getProperty("locale", "us"),
        prop.getProperty("obfkey", ""),
        prop.getProperty("prikey", "id")))

    } else {
      None
    }
  }

  def setMetadata(md: DatasetMeta) {
    val prop = new Properties()
    val propFile = internalToFile(md.datasetInternalName)
    prop.setProperty("copy", md.copy.toString)
    prop.setProperty("version", md.version.toString)
    prop.setProperty("locale", md.locale)
    prop.setProperty("obfkey", md.obfuscationKey)
    prop.setProperty("prikey", md.primaryKey)
    using(new FileOutputStream(propFile)) {
      fos => prop.store(fos, "Temporary Mapping for Secondary Store Dataset IDs")
    }


  }

}
