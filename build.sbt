name := "yhistorical"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.2",
  "org.apache.kafka" % "kafka_2.10" % "0.10.0.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


