
name := "Wikipedia-Translation-Toolkit"

version := "0.2"

scalaVersion := "2.12.12"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.7.1"


assemblyMergeStrategy in assembly := {
   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
   case x => MergeStrategy.first
}