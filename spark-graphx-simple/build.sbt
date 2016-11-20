lazy val root = (project in file(".")).
  settings(
    name := "spark-graphx-simple",
    version := "1.0",
    scalaVersion := "2.11.7"
 )

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-graphx_2.11" % sparkVersion
)

lazy val runSpark = taskKey[Unit]("Runs Spark in your machine")
runSpark := {
  val s: TaskStreams = streams.value
  s.log.info("Running apache spark...") 
  "./run-spark.sh" ! 
}
