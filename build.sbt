name := """hadoop-mr-demo"""

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "com.twitter" % "scalding-core_2.10" % "0.15.1-RC13")
