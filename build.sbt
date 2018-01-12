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
    scalacOptions ++= Seq(
      "–verbose",
      "-deprecation",           // Emit warning and location for usages of deprecated APIs.
      "-feature",               // Emit warning and location for usages of features that should be imported explicitly.
      "-unchecked",             // Enable additional warnings where generated code depends on assumptions.
      "-Xlint",                 // Enable recommended additional warnings.
      "-Ywarn-adapted-args",    // Warn if an argument list is modified to match the receiver.
      "-Ywarn-dead-code",
      "-Ywarn-value-discard",   // Warn when non-Unit expression results are unused.
      "-Ypartial-unification",
      "-language:postfixOps",
      "-language:higherKinds",
      "-language:existentials"
    ),
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