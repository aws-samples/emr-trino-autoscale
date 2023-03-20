name := "trino-autoscaling"
version := "0.1"
scalaVersion := "2.12.16"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

run / fork := true

val awsSdkVersion = "1.12.383"
val akkaVersion = "2.6.0"
val akkaHttpVersion = "10.1.11"
val commonsVersion = "2.11.0"
val json4sVersion = "4.0.6"
val log4jVersion = "2.19.0"

libraryDependencies ++= Seq(
  // AWS SDK
  "com.amazonaws" % "aws-java-sdk-emr" % awsSdkVersion,
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % awsSdkVersion,
  // Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  // commons-io
  "commons-io" % "commons-io" % commonsVersion,
  // json
  "org.json4s" %% "json4s-native" % json4sVersion,
  // log4j
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Runtime,
  // Tests
  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)

dependencyOverrides += "org.scala-lang.modules" % "scala-parser-combinators" % "1.1.2"

ThisBuild / mainClass := Some("com.amazonaws.emr.TrinoAutoscaler")
ThisBuild / assemblyMergeStrategy := {
  case PathList("io.netty.versions.properties") => MergeStrategy.discard
  case x if x.endsWith("/io.netty.versions.properties") => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}