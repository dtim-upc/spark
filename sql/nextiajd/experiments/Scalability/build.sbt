name := "Scalability"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"

libraryDependencies += "org.apache.spark" % "spark-sql" % "3.0.1"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/spark-sql_2.12-3.0.1.jar"

libraryDependencies += "org.apache.spark" % "spark-catalyst" % "3.0.1"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/spark-catalyst_2.12-3.0.1.jar"

libraryDependencies += "org.apache.spark" % "spark-nextiajd" % "3.0.1"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/spark-nextiajd_2.12-3.0.1.jar"

libraryDependencies += "org.apache.spark" % "spark-core" % "3.0.1"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/spark-core_2.12-3.0.1.jar"

libraryDependencies += "org.apache.log4j" % "log4j" % "1.2.17"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/log4j-1.2.17.jar"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"  % "provided" from "file:///usr/local/Cellar/apache-spark/3.0.1/libexec/jars/hadoop-common-3.2.0.jar"

libraryDependencies += "commons-io" % "commons-io" % "2.6"

//"org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy"),
//