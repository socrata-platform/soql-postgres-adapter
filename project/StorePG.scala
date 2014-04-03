import sbt._
import Keys._
import Dependencies._

object StorePG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly=true) ++ Seq(
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map CommonPG.genVersion,
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    secondarylib
  )
}


