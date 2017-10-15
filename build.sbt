
name := "spark-clickstream-analytics"

version := "1.0"
scalaVersion := "2.11.8"

sparkVersion := "2.2.0"
sparkComponents ++= Seq("sql", "streaming")

//spDependencies += "datastax/spark-cassandra-connector:2.0.1-s_2.11"

libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
//  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
//  "org.apache.spark" % "spark-hive_2.11" % "2.2.0",

  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0",
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.1",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1",

  "com.twitter" % "chill_2.11" % "0.9.2",
  "com.twitter" % "chill-avro_2.11" % "0.9.2",
  "com.twitter" % "algebird-core_2.11" % "0.13.3",

  "com.typesafe" % "config" % "1.3.1",
  "com.esotericsoftware" % "kryo" % "3.0.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}