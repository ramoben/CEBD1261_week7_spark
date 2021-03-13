name := "CEBD1261_week7_spark"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)
