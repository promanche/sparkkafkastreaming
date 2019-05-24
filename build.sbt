name := "sparkkafkastreaming"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-hive" % "2.0.0" % "test"
)
