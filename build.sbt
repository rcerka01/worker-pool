scalaVersion := "2.13.5"

organization := "com.pirum"
organizationName := "Pirum Systems"
organizationHomepage := Some(url("https://www.pirum.com"))

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "3.7.0",
  "org.typelevel" %% "log4cats-slf4j" % "2.6.0",
  "ch.qos.logback" % "logback-classic" % "1.4.8",
  "org.scalatest" %% "scalatest" % "3.2.16" % Test,
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
)
