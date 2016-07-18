package com.socrata.pg.store

import com.socrata.datacoordinator.secondary.SecondaryWatcherApp

object Main extends App {
  SecondaryWatcherApp(new PGSecondary(_))
}
