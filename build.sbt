name := "mole"

version := "0.1"

scalaVersion := "2.11.8"

lazy val commonSettings = Seq(
  organization := "org.freedomandy",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

lazy val common = (project in file("Common"))
  .settings(
    commonSettings//,
    // other settings
  )

lazy val plugings = (project in file("Plugins_Example")).dependsOn(common)
  .settings(
    commonSettings//,
    // other settings
  )

lazy val core = (project in file("Core")).dependsOn(common).dependsOn(plugings)
  .settings(
    commonSettings//,
    // other settings
  )