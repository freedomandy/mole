name := "mole"

version := "0.1"


lazy val commonSettings = Seq(
  organization := "org.freedomandy",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.11"
)

lazy val common = (project in file("Common"))
  .settings(
    commonSettings
  )
  .settings(libraryDependencies ++= Dependencies.common)

lazy val plugings = (project in file("Plugins_Example")).dependsOn(common)
  .settings(
    commonSettings
  )
  .settings(libraryDependencies ++= Dependencies.plugins)

lazy val core = (project in file("Core")).dependsOn(common).dependsOn(plugings)
  .settings(
    commonSettings
  )
  .settings(libraryDependencies ++= Dependencies.core)

cancelable in Global := true
