ThisBuild / fork in Test := true

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-Xfatal-warnings")

ThisBuild / organization := "com.socrata"

ThisBuild / resolvers += "socrata" at "https://repo.socrata.com/artifactory/libs-release"

val commonPG = project in file("common-pg")

val storePG = (project in file("store-pg")).
  dependsOn(commonPG % "compile;test->test")

// You look at this, and you think "why is there a commonPG and a storePG
// if soqlServerPG is just going to depend on both anyway?"  And you would
// be right to do so!  This has a dependency on store pg because the
// migrations are run by the soql-server container, but the _code_ to do
// so lives in storePG, but the _migrations themselves_ live in soqlServerPG.
// So, ugh... put it on the list of things to fix one day, but for now
// I just want to get staging unbroken again.  When that is fixed, remove
// "compile;" from the dependency spec below so that soqlServerPG only
// depends on storePG's test configuration.
val soqlServerPG = (project in file("soql-server-pg")).
  dependsOn(commonPG, storePG % "compile;test->test")

disablePlugins(AssemblyPlugin)

releaseProcess -= ReleaseTransformations.publishArtifacts
