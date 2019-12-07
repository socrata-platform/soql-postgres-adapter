ThisBuild / fork in Test := true

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings")

ThisBuild / organization := "com.socrata"

ThisBuild / resolvers += "socrata" at "https://repo.socrata.com/artifactory/libs-release"

val commonPG = project in file("common-pg")

val storePG = (project in file("store-pg")).
  dependsOn(commonPG % "test->test;compile->compile")

val soqlServerPG = (project in file("soql-server-pg")).
  dependsOn(storePG % "test->test;compile->compile")

disablePlugins(AssemblyPlugin)

releaseProcess -= ReleaseTransformations.publishArtifacts
