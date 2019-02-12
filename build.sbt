name := "NewYorkCityTaxi"

version := "0.1"

scalaVersion := "2.11.4"

resolvers += Resolver.bintrayRepo("jroper", "maven")

libraryDependencies ++= Seq (
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1"
  , "io.spray" %%  "spray-json" % "1.3.5"
  , "com.typesafe.play" %% "play-json" % "2.6.10"
  , "org.apache.spark" %% "spark-core" % "2.3.0"
  , "com.github.nscala-time" %% "nscala-time" % "2.20.0"
  , "org.scala-lang" % "scala-library" % "2.10.1"
  , "au.id.jazzy" %% "play-geojson" % "1.5.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
