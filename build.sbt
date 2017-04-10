name := """blackboard"""

lazy val commonDependencies = Seq(
  cache,
  javaWs,
  "org.neo4j" % "neo4j" % "3.1.0",
  "org.webjars" %% "webjars-play" % "2.5.0"
)

lazy val commonSettings = Seq(
  scalaVersion := "2.11.7",
  version := "1.0-SNAPSHOT"
)

lazy val javaBuildOptions = Seq(
  "-encoding", "UTF-8"
    //,"-Xlint:-options"
    //,"-Xlint:deprecation"
)

lazy val root = (project in file("."))
  .enablePlugins(PlayJava)
  .settings(commonSettings: _*)
  .settings(
  libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
)
