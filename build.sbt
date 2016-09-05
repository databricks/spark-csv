name := "spark-csv"

version := "1.5.0"

organization := "com.databricks"

scalaVersion := "2.11.7"

spName := "databricks/spark-csv"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.6.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.1",
  "com.univocity" % "univocity-parsers" % "1.5.1",
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/databricks/spark-csv</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:databricks/spark-csv.git</url>
    <connection>scm:git:git@github.com:databricks/spark-csv.git</connection>
  </scm>
  <developers>
    <developer>
      <id>falaki</id>
      <name>Hossein Falaki</name>
      <url>http://www.falaki.net</url>
    </developer>
  </developers>)

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

// -- MiMa binary compatibility checks ------------------------------------------------------------

import com.typesafe.tools.mima.core._
import com.typesafe.tools.mima.plugin.MimaKeys.binaryIssueFilters
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

mimaDefaultSettings ++ Seq(
  previousArtifact := Some("com.databricks" %% "spark-csv" % "1.2.0"),
  binaryIssueFilters ++= Seq(
    // These classes are not intended to be public interfaces:
    ProblemFilters.excludePackage("com.databricks.spark.csv.CsvRelation"),
    ProblemFilters.excludePackage("com.databricks.spark.csv.util.InferSchema"),
    ProblemFilters.excludePackage("com.databricks.spark.sql.readers"),
    ProblemFilters.excludePackage("com.databricks.spark.csv.util.TypeCast"),
    // We allowed the private `CsvRelation` type to leak into the public method signature:
    ProblemFilters.exclude[IncompatibleResultTypeProblem](
      "com.databricks.spark.csv.DefaultSource.createRelation")
  )
)

// ------------------------------------------------------------------------------------------------
