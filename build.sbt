name := "EventSourcing"

version := "1.0"

scalaVersion := "2.11.1"


libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.4"

libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.3.4"

libraryDependencies += "com.typesafe.akka" %% "akka-contrib" % "2.3.4"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6"


scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")