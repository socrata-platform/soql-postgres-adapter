package com.socrata.pg.store

import java.util.UUID

import com.socrata.datacoordinator.id.DatasetResourceName

trait TestResourceNames {
  def freshResourceNameRaw() = s"_test_${UUID.randomUUID().toString.toLowerCase}"
  def freshResourceName() = DatasetResourceName(freshResourceNameRaw())
}
