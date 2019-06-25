import sbt._

object Dependencies {

  def compiledDependencies(deps: ModuleID*): Seq[ModuleID] = deps.map(_ % Compile)

  def testDependencies(deps: ModuleID*): Seq[ModuleID] = deps.map(_ % Test)

  val sparkVersion = "2.4.3"

  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  val sparSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
  val typeSafeConfig = "com.typesafe" % "config" % "1.3.4"
  val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val javaXMail = "javax.mail" % "mail" % "1.4"

  //test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7"
}