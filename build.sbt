
lazy val branch = "git rev-parse --abbrev-ref HEAD".!!.trim
lazy val commit = "git rev-parse --short HEAD".!!.trim
lazy val author = s"git show --format=%an -s $commit".!!.trim
lazy val buildDate = (new java.text.SimpleDateFormat("yyyyMMdd"))
  .format(new java.util.Date())
lazy val appVersion = "%s-%s-%s".format(branch, buildDate, commit)

lazy val commonDependencies = Seq(
  cache,
  javaWs,
  "org.neo4j" % "neo4j" % "3.1.0",
  "org.webjars" %% "webjars-play" % "2.5.0",
  "org.webjars" % "bootstrap" % "3.3.7",
  "org.webjars" % "jquery" % "3.2.0",
  "org.webjars" % "datatables" % "1.10.12",
  "org.webjars" % "datatables-plugins" % "1.10.12",
  "org.webjars" % "datatables-bootstrap" % "2-20120202-2"
)

lazy val javaBuildOptions = Seq(
  "-encoding", "UTF-8"
    //,"-Xlint:-options"
    //,"-Xlint:deprecation"
)

lazy val commonSettings = Seq(
  name := """blackboard""",
  scalaVersion := "2.11.8",
  version := appVersion
)

lazy val root = (project in file("."))
  .enablePlugins(PlayJava)
  .settings(commonSettings: _*)
  .dependsOn(buildinfo,core,pharos,biothings)
  .aggregate(buildinfo,core,pharos,biothings)

lazy val buildinfo = (project in file("modules/build"))
  .settings(commonSettings: _*)
  .settings(name := "blackboard-buildinfo",
    sourceGenerators in Compile <+= sourceManaged in Compile map { dir =>
      val file = dir / "BuildInfo.java"
      IO.write(file, """
package blackboard;
public class BuildInfo { 
   public static final String BRANCH = "%s";
   public static final String DATE = "%s";
   public static final String COMMIT = "%s";
   public static final String TIME = "%s";
   public static final String AUTHOR = "%s";
}
""".format(branch, buildDate, commit, new java.util.Date(), author))
      Seq(file)
    }
)

lazy val core =  (project in file("modules/core"))
  .settings(commonSettings: _*)
  .settings(
    name := "blackboard-core",
    libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
)

lazy val pharos = (project in file("modules/pharos"))
  .enablePlugins(PlayJava)
  .settings(commonSettings: _*)
  .settings(
    name := "blackboard-pharos",
    libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
).dependsOn(core).aggregate(core)

lazy val biothings = (project in file("modules/biothings"))
  .settings(commonSettings: _*)
  .settings(
    name := "blackboard-biothings",
    libraryDependencies ++= commonDependencies,
    javacOptions ++= javaBuildOptions
).dependsOn(core).aggregate(core)
