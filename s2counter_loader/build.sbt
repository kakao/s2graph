import sbtassembly.Plugin.AssemblyKeys._

name := "s2counter-loader"

scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % Common.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % Common.sparkVersion,
  "com.typesafe.play" %% "play-ws" % Common.playVersion,
  "org.specs2" %% "specs2-core" % "3.6" % "test"
).map { id =>
  id.excludeAll(ExclusionRule(organization = "javax.servlet"), ExclusionRule(organization = "org.mortbay.jetty"), ExclusionRule(organization = "com.google.guava"))
}

// force specific library version
libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "16.0.1"
)

fork := true

transitiveClassifiers ++= Seq()

assemblySettings

mergeStrategy in assembly := {
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}
