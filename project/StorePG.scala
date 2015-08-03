import Dependencies._
import sbt.Keys._
import sbt._

object StorePG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies <++= scalaVersion(libraries(_))
  )

  def libraries(implicit scalaVersion: String) = Seq(
    secondarylib
  )
}


