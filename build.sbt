import org.typelevel.sbt.tpolecat.*

ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "3.4.0"
name := "db-copy"

// This disables fatal-warnings for local development. To enable it in CI set the `SBT_TPOLECAT_CI` environment variable in your pipeline.
// See https://github.com/typelevel/sbt-tpolecat/?tab=readme-ov-file#modes
ThisBuild / tpolecatDefaultOptionsMode := VerboseMode

val testcontainersVersion = "0.44.0"

lazy val root = (project in file(".")).settings(
  name := "interview",
  libraryDependencies ++= Seq(
    // "core" module - IO, IOApp, schedulers
    // This pulls in the kernel and std modules automatically.
    "org.typelevel" %% "cats-effect" % "3.5.3",
    // concurrency abstractions and primitives (Concurrent, Sync, Async etc.)
    "org.typelevel" %% "cats-effect-kernel" % "3.5.3",
    // standard "effect" library (Queues, Console, Random etc.)
    "org.typelevel" %% "cats-effect-std" % "3.5.3",

    // FS2
    "co.fs2" %% "fs2-core" % "3.12.0",
    "co.fs2" %% "fs2-io" % "3.12.0",

    // Logging
    "ch.qos.logback" % "logback-classic" % "1.5.27",

    // Test
    "org.scalamock" %% "scalamock" % "7.5.3",
    "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
    "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
    "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.7.0" % Test
  ),
  Compile / mainClass := Some("com.example.Main")
).enablePlugins(JavaAppPackaging)