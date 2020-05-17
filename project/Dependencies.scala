import sbt._

object Dependencies {
  private val SCALA_VERSION = "2.12.11"

  private val SPARK_VERSION = "2.4.4"
  private val SCALA_TEST_VERSION = "3.0.1"
  private val MONGO_SPARK_VERSION = "2.4.1"
  private val TYPESAFE_CONFIG_VERSION = "1.3.0"
  private val ELASTICSEARCH_VERSION = "6.1.2"
  private val APACHE_HTTP_VERSION = "4.5.2"
  private val KAFKA_VERSION = "2.3.0"


  val core: Seq[ModuleID] = Seq(
      "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
      "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
      //"org.apache.spark" %% "spark-hive" % SPARK_VERSION % Test,
      "com.typesafe" % "config" % TYPESAFE_CONFIG_VERSION,
      "org.mongodb.spark" %% "mongo-spark-connector" % MONGO_SPARK_VERSION,
      "org.scalatest" %% "scalatest" % SCALA_TEST_VERSION,
      "org.elasticsearch" % "elasticsearch-hadoop" % ELASTICSEARCH_VERSION,
      "org.apache.httpcomponents" % "httpclient" % APACHE_HTTP_VERSION,
      "com.h2database" % "h2" % "1.4.196" % Runtime,
      "org.scala-lang" % "scala-compiler" % SCALA_VERSION
  )

  val common: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    "com.typesafe" % "config" % TYPESAFE_CONFIG_VERSION,
    "org.scalatest" %% "scalatest" % SCALA_TEST_VERSION,
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
    "org.apache.httpcomponents" % "httpclient" % APACHE_HTTP_VERSION,
    "org.apache.kafka" % "kafka-clients" % KAFKA_VERSION
  )

  val plugins: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
    "org.mongodb.spark" %% "mongo-spark-connector" % MONGO_SPARK_VERSION  % "provided",
    "com.typesafe" % "config" % TYPESAFE_CONFIG_VERSION  % "provided",
    "org.scalatest" %% "scalatest" % SCALA_TEST_VERSION % Test
  )
}
