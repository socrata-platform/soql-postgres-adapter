import com.socrata.sbtplugins.BuildInfoPlugin
import sbt._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql-postgres-adapter",
    file(".")
  ).settings(BuildSettings.buildSettings: _*)
   .aggregate(commonPG, storePG, soqlServerPG)
   .dependsOn(commonPG, storePG, soqlServerPG)

  def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).dependsOn(dependencies: _*)

  lazy val commonPG = p("common-pg", CommonPG) enablePlugins BuildInfoPlugin
  lazy val storePG = p("store-pg", StorePG) dependsOn(commonPG % "test->test;compile->compile")
  lazy val soqlServerPG = p("soql-server-pg", SoqlServerPG) dependsOn(storePG % "test->test;compile->compile")
}
