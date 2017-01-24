name := "parquet-readwrite"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-hadoop" % "1.9.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3"
)
