scalaVersion := "2.10.4"

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.2.0")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.1")

// For Sonatype publishing
//resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

//addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.6")

addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")

libraryDependencies += "org.ow2.asm"  % "asm" % "5.0.3"

libraryDependencies += "org.ow2.asm"  % "asm-commons" % "5.0.3"
