lazy val root = (project in file(".")).
  settings(
    name := "spark-simple",
    version := "1.0",
    scalaVersion := "2.11.7"
 )

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

scalacOptions += "-feature"
initialCommands in console := "import scala._"
