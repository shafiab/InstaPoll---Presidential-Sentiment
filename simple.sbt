name := "Presidential Sentiment"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"
unmanagedBase := baseDirectory.value / "lib"