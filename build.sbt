import Dependencies._

name := "spark-kafka-vertica"

version := "0.1"

scalaVersion := "2.11.8"

unmanagedJars in Compile ++= Seq(
  baseDirectory.value / "lib/vertica-9.0.1_spark2.1_scala2.11.jar",
  baseDirectory.value / "lib/vertica-jdbc-8.1.1-5.jar")

libraryDependencies ++= compiledDependencies(
  sparkSql,
  sparSqlKafka,
  logback,
  typeSafeConfig,
)++ testDependencies(
  scalaTest
)