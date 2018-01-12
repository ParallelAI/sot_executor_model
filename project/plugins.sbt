resolvers ++= Seq[Resolver](
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Era7 maven releases" at "https://s3-eu-west-1.amazonaws.com/releases.era7.com",
  Resolver.sbtPluginRepo("releases"),
  Classpaths.sbtPluginReleases
)

// addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0") TODO - Need extra coursier to work with S3

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("ohnosequences" % "sbt-s3-resolver" % "0.16.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")