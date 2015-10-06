name := "Warehouse"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.0"
libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4-1200-jdbc41"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
