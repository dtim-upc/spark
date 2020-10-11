name := "NextiaJD"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" exclude("org.apache.spark", "spark-catalyst_2.12") exclude("org.apache.spark", "spark-sql_2.12")

libraryDependencies += "org.antlr" % "antlr4-runtime" % "4.7.1"
libraryDependencies += "org.codehaus.janino" % "commons-compiler" % "3.0.16"
libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.16"
libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.10.1"
libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.10.1"
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.9.0"
