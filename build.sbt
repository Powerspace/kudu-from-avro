import NativePackagerHelper._

name := "kudu-from-avro"

version := "1.0"

organization := "com.powerspace"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-feature", "-deprecation", "-encoding", "utf8", "-Ywarn-dead-code", "-Ywarn-numeric-widen", "-Ywarn-unused", "-Ywarn-unused-import" /*,"-Ymacro-debug-lite"*/)

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"
libraryDependencies += "org.apache.kudu" % "kudu-client" % "1.1.0"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.22"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.22"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

dependencyOverrides += "org.slf4j" % "slf4j-api" % "1.7.22"

testOptions in Test += Tests.Argument("-oDF")

enablePlugins(JavaAppPackaging)