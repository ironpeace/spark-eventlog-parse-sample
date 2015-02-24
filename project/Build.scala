import sbt._
import sbt.Keys._

object Build extends Build {
  lazy val graphxsamples = Project(
    id = "spark-eventlog-parse-sample",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "spark-eventlog-parse-sample",
      organization := "com.teppeistudio",
      version := "1.0",
      scalaVersion := "2.10.4",
      libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0",
      libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.1.0",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"
    )
  )
}
