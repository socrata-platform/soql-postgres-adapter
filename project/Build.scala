import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soql-postgres-adapter",
    file(".")
  ).settings(BuildSettings.buildSettings: _*)
   .aggregate(commonPG, storePG, soqlServerPG)
   .dependsOn(commonPG, storePG, soqlServerPG)

  /*
  def allOtherProjects =
    for {
      method <- getClass.getDeclaredMethods.toSeq
      if method.getParameterTypes.isEmpty && classOf[Project].isAssignableFrom(method.getReturnType) && method.getName != "build"
    } yield method.invoke(this).asInstanceOf[Project] : ProjectReference
  */

  def p(name: String, settings: { def settings: Seq[Setting[_]] }, dependencies: ClasspathDep[ProjectReference]*) =
    Project(name, file(name)).settings(settings.settings : _*).dependsOn(dependencies: _*)

  val commonPG = p("common-pg", CommonPG)
  val storePG = p("store-pg", StorePG) dependsOn(commonPG % "test->test;compile->compile")
  val soqlServerPG = p("soql-server-pg", SoqlServerPG) dependsOn(storePG % "test->test;compile->compile")
}
