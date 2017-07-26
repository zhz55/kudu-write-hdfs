name := "kudu-write-hdfs"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % "provided",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.2.0"
)