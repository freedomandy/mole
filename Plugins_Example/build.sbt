name := "mole_plugin_example"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  Seq(
    "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.1.2",
    "com.typesafe" % "config" % "1.3.0",
    "org.scalatest" %% "scalatest" % "3.0.1"
  )
}