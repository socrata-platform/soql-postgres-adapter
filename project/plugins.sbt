resolvers ++= Seq(
  Resolver.url("socrata ivy", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")
