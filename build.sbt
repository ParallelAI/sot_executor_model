import Dependencies._
import com.amazonaws.regions.{Region, Regions}

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "parallelai",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "sot_executor_model",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "io.spray" %% "spray-json" % "1.3.3"
    )
  )

resolvers ++= Seq[Resolver](
  s3resolver.value("Parallel AI S3 Releases resolver", s3("release.repo.parallelai.com")),
  s3resolver.value("Parallel AI S3 Snapshots resolver", s3("snapshot.repo.parallelai.com"))
)

publishMavenStyle := false
s3region := Region.getRegion(Regions.EU_WEST_2)
publishTo := {
  val prefix = if (isSnapshot.value) "snapshot" else "release"
  Some(s3resolver.value("Parallel AI "+prefix+" S3 bucket", s3(prefix+".repo.parallelai.com")) withIvyPatterns)
}

crossScalaVersions := Seq("2.11.11", "2.12.3")