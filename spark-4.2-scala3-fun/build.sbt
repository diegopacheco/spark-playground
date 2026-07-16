ThisBuild / scalaVersion := "3.7.4"
ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-42-scala3-fun",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % "4.2.0").cross(CrossVersion.for3Use2_13)
    ),
    scalacOptions ++= Seq("-deprecation", "-feature"),
    run / fork := true,
    javaOptions ++= Seq(
      "--add-modules=jdk.incubator.vector",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--enable-native-access=ALL-UNNAMED"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
      case "module-info.class"                       => MergeStrategy.discard
      case x if x.endsWith(".proto")                 => MergeStrategy.first
      case x                                          => MergeStrategy.first
    },
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := "spark-42-scala3-fun.jar"
  )
