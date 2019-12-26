name := "teradata-ingestion"
organization := "com.axa.datalab"
releaseUseGlobalVersion := false

scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.logging.log4j" % "log4j-api" % "2.10.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.10.0",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

)