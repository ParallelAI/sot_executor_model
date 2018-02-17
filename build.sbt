import scala.language.postfixOps
import com.amazonaws.regions.{Region, Regions}
import Dependencies._

lazy val scala_2_11 = "2.11.11"
lazy val scala_2_12 = "2.12.4"

lazy val root = (project in file("."))
  .settings(
    name := "sot_executor_model",
    inThisBuild(List(
      organization := "parallelai",
      scalaVersion := scala_2_11
    )),
    crossScalaVersions := Seq(scala_2_11, scala_2_12),
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
      shapeless,
      sprayJson
    )
  )