name := "spark-clickstream-analytics"

version := "1.0"
scalaVersion := "2.11.8"


libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.esotericsoftware" % "kryo" % "3.0.3"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)