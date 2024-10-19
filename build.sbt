name := "SparkSearchEngine"

version := "0.1"

scalaVersion := "2.12.17" // Use a Scala version compatible with Spark 3.5.0

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "5.0.0",
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
