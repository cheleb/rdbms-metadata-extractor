import sbt._
import sbt.Keys._

object RdbmsmetadataextractorBuild extends Build {

  lazy val rdbmsmetadataextractor = Project(
    id = "rdbms-metadata-extractor",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "rdbms-metadata-extractor",
      organization := "net.orcades.db",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.2"
      // add other settings here
    )
  )
}
