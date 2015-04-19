name := "spark-csv"

version := "1.0.3"

organization := "com.databricks"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" % "provided"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

publishMavenStyle := true

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
      <name>Apache License, Verision 2.0</name>
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

sparkPackageName := "databricks/spark-csv"

sparkVersion := "1.3.0"

sparkComponents += "sql"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

// Fork to help tests of methods using reflection.
// See https://issues.apache.org/jira/browse/SPARK-5281.
fork in Test := true
