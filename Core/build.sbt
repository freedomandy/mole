name := "mole_core"

version := "0.1"

scalaVersion := "2.12.11"

resolvers += Resolver.mavenLocal
unmanagedBase := baseDirectory.value / "src" / "lib"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org","objenesis", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "sun", xs @ _*) => MergeStrategy.last
  case PathList("oracle", "jdbc", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.rename
  case "plugin.xml" => MergeStrategy.rename
  case "parquet.thrift" => MergeStrategy.rename
  case "git.properties" => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
