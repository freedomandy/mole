name := "mole_common"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  Seq(
    "com.typesafe" % "config" % "1.3.0",
    "org.scalatest" %% "scalatest" % "3.0.1",
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.httpcomponents" % "httpclient" % "4.5.2"
  )
}