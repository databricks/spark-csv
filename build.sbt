name := "spark-csv"

version := "1.1.0-SNAPSHOT"

organization := "com.databricks"

scalaVersion := "2.10.4"

parallelExecution in Test := false

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.1"

libraryDependencies += "com.univocity" % "univocity-parsers" % "1.5.1"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" % "provided"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "1.4.0").
//    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("org.slf4j", "slf4j-api").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("com.esotericsoftware.minlog", "minlog")
)


resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

publishMavenStyle := true

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"
