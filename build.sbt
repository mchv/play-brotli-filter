resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"


val Scala212 = "2.12.20"
val Scala213 = "2.13.15"
val Scala3 = "3.3.4"


ThisBuild / scalaVersion := Scala213

lazy val sharedSettings = Seq(
  /* sbt 2 needs Java 17+, so build for Java 11 explicitly to keep the artifacts usable on Java 11 */
  scalacOptions ++= Seq("-feature", "-deprecation", "-release", "11"),
  javacOptions ++= Seq("--release", "11"),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.17" % Test
  ),
  Test / publishArtifact := false,
  publishConfiguration := publishConfiguration.value.withOverwrite(true)
)

val Brotli4jVersion = "1.24.0"
lazy val common = project
  .settings(
    name:="brotli4s",
    description := "A scala wrapper for brotli4j",
    sharedSettings,
    crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    libraryDependencies ++= Seq(
      "com.aayushatharva.brotli4j" % "brotli4j" % Brotli4jVersion
    ),

    artifactPomMetadataSettings,

    assemblySettings,

    /* Dependent projects must use the class directory. Under sbt 2 they otherwise use the packageBin jar, which is
       replaced by the assembly task, and building akka/pekko/play-* then hangs (a cycle through assembly's classpath). */
    exportJars := false
  )

val AkkaVersion = "2.6.21"
lazy val akka = project.dependsOn(common)
  .settings(
    name:="akka-stream-brotli",
    description := "A brotli stream extension for Akka 2.6",
    sharedSettings,
    crossScalaVersions := Seq(Scala212, Scala213, Scala3),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion % Provided,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    ),

    artifactPomMetadataSettings


  )

val PekkoVersion = "1.0.3"
lazy val pekko = project.dependsOn(common)
  .settings(
    name:="pekko-stream-brotli",
    description := "A brotli stream extension for Pekko 1.0",
    sharedSettings,
    crossScalaVersions := Seq(Scala213, Scala3),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream" % PekkoVersion % Provided,
      "org.apache.pekko" %% "pekko-stream-testkit" % PekkoVersion % Test,
    ),

    artifactPomMetadataSettings

  )


val artifactPomMetadataSettings = Seq(
  organization := "io.github.mchv",
  licenses := Seq("Apache V2" -> uri("https://www.apache.org/licenses/LICENSE-2.0.html")),
  scmInfo := Some(ScmInfo(
  uri("https://github.com/mchv/play-brotli-filter"),
    "scm:git:git@github.com:mchv/play-brotli-filter.git"
  )),
  pomExtra := {
    <url>https://github.com/mchv/play-brotli-filter</url>
      <developers>
        <developer>
          <id>mchv</id>
          <name>Mariot Chauvin</name>
          <url>https://github.com/mchv</url>
        </developer>
      </developers>
  }
)

val assemblySettings = Seq( 
    /* Use assembly output as packaged jar */
    Compile / packageBin := assembly.value,


    /* Keep the version out of the jar name: sbt 2 caches the assembly output, and a name that contains the
       version makes a cached jar from another version (e.g. a snapshot) be picked up and fail signing */
    assembly / assemblyJarName :=  s"${name.value}_${scalaBinaryVersion.value}.jar",

    /* Exclude the scala library from being included in assembled jar*/
    assembly / assemblyOption ~= {
          _.withIncludeScala(false)
    },

    /* Exclude brotli4j and the Scala libraries from being included in assembled jar.
       withIncludeScala(false) only covers scala-library, not scala3-library_3. */
    assembly / assemblyExcludedJars := {
          val cp = (assembly / fullClasspath).value
          cp filter { f =>
            val jarName = f.data.name
            jarName == s"brotli4j-${Brotli4jVersion}.jar" || jarName.startsWith("scala3-library_3-")
          }
    },


    assembly / assemblyMergeStrategy := {
        case PathList("META-INF", "versions", xs @ _, "module-info.class") => MergeStrategy.discard
        case x =>
            val oldStrategy = (assembly / assemblyMergeStrategy).value
            oldStrategy(x)
    }
  )

lazy val `play-v28` = project
  .dependsOn(akka)
  .settings(
    sharedSettings,
    name:="play-v28-brotli-filter",
    description := "A brotli filter module for Play 2.8",
    crossScalaVersions := Seq(Scala212, Scala213),
    /* Test deps (scalatest 3.2 needs scala-xml 2.x, Play 2.8 needs 1.x) conflict on Scala 2.12. Tests ran with
       scala-xml 2.x under sbt 1, where this was only a warning; sbt 2 turns it into an error. */
    libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always,
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play" % "2.8.22" % Provided,
      "com.typesafe.play" %% "filters-helpers" % "2.8.22" % Test,
      "com.typesafe.play" %% "play-specs2" % "2.8.22" % Test,
      "ch.qos.logback" % "logback-classic" % "1.4.14"  % Test,
      /* override guice version below to fix reflection issue see https://github.com/google/guice/issues/1133 */
      "com.google.inject" % "guice" % "6.0.0" % Test,
      "com.google.inject.extensions" % "guice-assistedinject" % "6.0.0" % Test
    ),

    artifactPomMetadataSettings
  )

lazy val `play-v29` = project
  .dependsOn(akka)
  .settings(
    sharedSettings,
    name:="play-v29-brotli-filter",
    description := "A brotli filter module for Play 2.9",
    crossScalaVersions := Seq(Scala213, Scala3),
    libraryDependencies ++= Seq( 
      "com.typesafe.play" %% "play" % "2.9.6" % Provided,
      "com.typesafe.play" %% "filters-helpers" % "2.9.0-M6" % Test,
      "com.typesafe.play" %% "play-specs2" % "2.9.6" % Test,
    ),

    artifactPomMetadataSettings
  )

  lazy val `play-v30` = project
  .dependsOn(pekko)
  .settings(
    sharedSettings,
    name:="play-v30-brotli-filter",
    description := "A brotli filter module for Play 3",
    crossScalaVersions := Seq(Scala213, Scala3),
    libraryDependencies ++= Seq( 
      "org.playframework" %% "play" % "3.0.6" % Provided,
      "org.playframework" %% "play-filters-helpers" % "3.0.6" % Test,
      "org.playframework" %% "play-specs2" % "3.0.6" % Test,
    ),

    artifactPomMetadataSettings
  )


lazy val `play-brotli-filter-root` = (project in file("."))
  .aggregate(common, akka, pekko, `play-v28`,`play-v29` ,`play-v30`)
  .settings(
    publish / skip := true,
    crossScalaVersions := Nil,

    organization := "io.github.mchv"
  )