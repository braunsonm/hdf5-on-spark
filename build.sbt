name := "hdf5-on-spark"

version := "1.0"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1" % "provided",
  "hdf.hdf5lib" % "*" % "1.10.5" from s"file:///${baseDirectory.value.getAbsolutePath}/lib/jarhdf5-1.10.5.jar",
  "ch.systemsx.cisd.hdf5" % "*" % "14.12" from s"file:///${baseDirectory.value.getAbsolutePath}/lib/sis-jhdf5-batteries_included.jar",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.4.2",
  // https://mvnrepository.com/artifact/com.alibaba/fastjson
  "com.alibaba" % "fastjson" % "1.2.62",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
