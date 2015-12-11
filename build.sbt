name := "EventSourcing"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.1"

libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.4.1"

libraryDependencies += "com.typesafe.akka" %% "akka-contrib" % "2.4.1"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.0"

libraryDependencies += "com.chuusai" %% "shapeless" % "2.2.5"


libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.1" % "test"


resolvers += "migesok at bintray" at "http://dl.bintray.com/migesok/maven"


scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")