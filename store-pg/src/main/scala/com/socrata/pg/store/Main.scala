package com.socrata.pg.store

import com.socrata.datacoordinator.secondary.SecondaryWatcherApp
import com.socrata.pg.config.StoreConfig

object Main extends App {
  SecondaryWatcherApp(config => new PGSecondary(new StoreConfig(config, "")))
}
