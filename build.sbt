import scala.language.postfixOps
import com.amazonaws.regions.{Region, Regions}
import Dependencies._

lazy val root = (project in file("."))
  .settings(
    name := "sot_executor_model",
    inThisBuild(List(
      organization := "parallelai",
      scalaVersion := "2.11.11"
    )),
    crossScalaVersions := Seq("2.11.11", "2.12.3"),
    s3region := Region.getRegion(Regions.EU_WEST_2),
    publishTo := {
      val prefix = if (isSnapshot.value) "snapshot" else "release"
      Some(s3resolver.value(s"Parallel AI $prefix S3 bucket", s3(s"$prefix.repo.parallelai.com")) withMavenPatterns)
    },
    resolvers ++= Seq[Resolver](
      s3resolver.value("Parallel AI S3 Releases resolver", s3("release.repo.parallelai.com")) withMavenPatterns,
      s3resolver.value("Parallel AI S3 Snapshots resolver", s3("snapshot.repo.parallelai.com")) withMavenPatterns
    ),
    resolvers += sbtResolver.value,
    libraryDependencies ++= Seq(
      scalaTest % Test
    ),
    libraryDependencies ++= Seq(
      sprayJson
    )
  )