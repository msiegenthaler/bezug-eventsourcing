name := "EventSourcing"

version := "1.0"

scalaVersion := "2.11.2"


libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.4"

libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.3.4"

libraryDependencies += "com.typesafe.akka" %% "akka-contrib" % "2.3.4"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6"

libraryDependencies += "com.chuusai" %% "shapeless" % "2.0.0"


libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.4" % "test"

libraryDependencies += "com.github.michaelpisula" %% "akka-persistence-inmemory" % "0.2.1" % "test"


resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"


scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")