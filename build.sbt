name := "spark_project_kickstarter_2020_2021"

version := "1.0"

organization := "paristech"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

//In case you use %% syntax (after the groupId) in sbt, it automatically picks up the artifact for your
// scala version. So using scala 2.10 it changes your spark-cassandra-connector to spark-cassandra-connector_2.10.
// Not sure this feature is there when using spark-shell, so you might need to ask for the scala2_10 version of your
// artifact explicitly like this: --packages "com.datastax.spark:spark-cassandra-connector_2.10:1.6.0-M1"
libraryDependencies ++= Seq(
  // Spark dependencies. Marked as provided because they must not be included in the uber jar
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Third-party libraries
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0" % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"
  //"com.github.scopt" %% "scopt" % "3.4.0"        // to parse options given to the jar in the spark-submit
)

// https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.0-rc0"

// A special option to exclude Scala itself from our assembly JAR, since Spark already bundles Scala.
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

// Disable parallel execution because of spark-testing-base
Test / parallelExecution := false

// Configure the build to publish the assembly JAR
(Compile / assembly / artifact) := {
  val art = (Compile / assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(Compile / assembly / artifact, assembly)
