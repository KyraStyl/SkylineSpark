name := "sparkSkyline_TopK"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"


libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.10" % "2.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  //"org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
