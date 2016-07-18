package com.socrata.pg.store

import com.socrata.datacoordinator.secondary.SecondaryWatcherApp

object Main extends App {
  if(args.headOption == Some("--migrate")) {
    MigrateSchema(args.drop(1))
  } else {
    SecondaryWatcherApp(new PGSecondary(_))
  }
}
