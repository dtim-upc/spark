import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, lit, udf, when}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.rogach.scallop.ScallopConf


object EvaluateDiscovery {

  val highJoin = Seq(3, 4) // label 1
  val lowJoin = Seq(0, 1, 2) // label 0

  def getStats(spark:SparkSession, discovery: DataFrame, solution: String = ""): String = {

    import spark.implicits._

    val predLabels = discovery.select("prediction","trueQuality")
      .as[(Double, Double)].rdd

    val divider = "-----------------------------------------------\n\n"
    var lines = divider +  s"Summary Statistics ${solution} \n" + divider
    val mMetrics = new MulticlassMetrics(predLabels)

    val accuracy = mMetrics.accuracy
    lines = lines + s"Accuracy = $accuracy \n"

    // Precision by label
    val labels = mMetrics.labels
    labels.foreach { l =>
      lines = lines + s"Precision($l) = ${mMetrics.precision(l) }\n"
    }
    lines = lines + "\n"
    // Recall by label
    labels.foreach { l =>
      lines = lines + s"Recall($l) =  + ${mMetrics.recall(l)} \n"
    }
    lines = lines + "\n"
    // False positive rate by label
    labels.foreach { l =>
      lines = lines + s"FPR($l) =  ${mMetrics.falsePositiveRate(l)} \n"
    }
    lines = lines + "\n"
    // F-measure by label
    labels.foreach { l =>
      lines = lines + s"F1-Score($l) =  + ${mMetrics.fMeasure(l)} \n"
    }

    val matrix = mMetrics.confusionMatrix.toString().split("\n")
    if (matrix.size == 2) {
      lines = lines + "\nConfusion matrix:\nPredictions -> \t0\t\t1 " +
        s"\nTrue label 0:  ${matrix(0)}" +
        s"\nTrue label 1:  ${matrix(1)}\n"
    } else {
      lines = lines + "\nConfusion matrix:\nPredictions -> \t0\t\t1\t  2\t\t 3\t 4 " +
        s"\nTrue label 0:  ${matrix(0)}" +
        s"\nTrue label 1:  ${matrix(1)}" +
        s"\nTrue label 2:  ${matrix(2)}" +
        s"\nTrue label 3:  ${matrix(3)}" +
        s"\nTrue label 4:  ${matrix(4)}\n"
    }

    lines
  }

  def run(spark: SparkSession, nextia: DataFrame, flex: DataFrame, lsh: DataFrame, output: String): Unit = {


    val nextiaDF = nextia
      .withColumn("prediction",
        when(col("quality") === "High", lit(4D))
          .when(col("quality") === "Good", lit(3D))
          .when(col("quality") === "Moderate", lit(2D))
          .when(col("quality") === "Poor", lit(1D))
          .otherwise(lit(0D))
      )

    val nextiaBinary = nextiaDF.withColumn("prediction",
      when(col("prediction").isin(highJoin: _*), lit(1))
        .otherwise(lit(0))
    ).withColumn("trueQuality",
      when(col("trueQuality").isin(highJoin: _*), lit(1))
        .otherwise(lit(0))
    )

    var lines = "For binary matrix, true labels are: " +
      "\n label 1: Qualities 4 and 3 \n label 0: Qualities: 0, 1 and 2 \n\n"
    lines = lines+  getStats(spark, nextiaBinary, "NextiaJD Binary")

    val flexDF = flex.withColumn("prediction", lit(1))

    val dsFlex = nextiaBinary.select("query dataset","query attribute","candidate dataset","candidate attribute","trueQuality")
      .join(flexDF,Seq("query dataset","query attribute","candidate dataset","candidate attribute"),"left_outer").na.fill(0)

    lines = lines + getStats(spark, dsFlex, "FlexMatcher")

    val dsLSH = nextiaBinary.select("sizeDistinct1","sizeDistinct2","query dataset","query attribute","candidate dataset","candidate attribute","trueQuality","trueContainment")
      .join(lsh,Seq("query dataset","query attribute","candidate dataset","candidate attribute"),"left_outer")
      .groupBy("query dataset","query attribute","candidate dataset","candidate attribute", "trueQuality","trueContainment","sizeDistinct1","sizeDistinct2")
      .agg(org.apache.spark.sql.functions.max("threshold").as("threshold")).na.fill(0)

      .withColumn("prediction",
        when(col("threshold") === "0.75", lit(1D))
          .when(col("threshold") === "0.5", lit(1D))
          .when(col("threshold") === "0.25", lit(0D))
          .when(col("threshold") === "0.10", lit(0D))
          .otherwise(lit(0D))
      )

    val fullLSH = nextiaDF.select("sizeDistinct1","sizeDistinct2","query dataset","query attribute","candidate dataset","candidate attribute","trueQuality","trueContainment")
      .join(lsh,Seq("query dataset","query attribute","candidate dataset","candidate attribute"),"left_outer")
      .groupBy("query dataset","query attribute","candidate dataset","candidate attribute", "trueQuality","trueContainment","sizeDistinct1","sizeDistinct2")
      .agg(org.apache.spark.sql.functions.max("threshold").as("threshold")).na.fill(0)
//      .withColumn("prediction", setQuality(col("threshold"),col("sizeDistinct1"), col("sizeDistinct2")))

      .withColumn("prediction",
        when(col("threshold") === "0.75", lit(4D))
          .when(col("threshold") === "0.5", lit(3D))
          .when(col("threshold") === "0.25", lit(2D))
          .when(col("threshold") === "0.10", lit(1D))
          .otherwise(lit(0D))
      )


    lines = lines + getStats(spark, dsLSH, "LSH Ensemble Binary")

    lines = lines + getStats(spark, nextiaDF, "NextiaJD Full matrix")
    lines = lines + getStats(spark, fullLSH, "LSH Ensemble Full matrix")

    val outputfile = output+s"/Comparison_state_of_the_art.txt"
    println(lines)
    println("")
    println(s"Writting comparison in file: ${outputfile}")
    writeFile(outputfile, Seq(lines))

  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

  def setLabel(containment: Double ,cardinalityQ: Double, cardinalityC: Double): Double = {

    if (cardinalityQ*4 >= cardinalityC && containment >= 0.75) return 4D

    if (cardinalityQ*8 >= cardinalityC && containment >= 0.5) return 3D

    if (cardinalityQ*12 >= cardinalityC && containment >= 0.25) return 2D

      if (containment >= 0.1) {
        1D
      } else {
        0D
      }

  }

  val setQuality = udf(setLabel(_: Double, _: Double, _: Double): Double)

  def main(args: Array[String]): Unit = {
    val conf = new ConfEval(args)

    val spark = SparkSession.builder().master("local[*]")
      .config("spark.driver.memory", "9g").config("spark.driver.maxResultSize", "9g")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var nextiaDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(conf.nextiajd().getAbsolutePath)

    var flexDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(conf.flexmatcher().getAbsolutePath)

    var lshDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(conf.lsh().getAbsolutePath)


    if(conf.nextiajdM() != "NoProvided"){
      nextiaDF = nextiaDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.nextiajdM()))
    }
    if(conf.nextiajdS() != "NoProvided"){
      nextiaDF = nextiaDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.nextiajdS()))
    }
    if(conf.lshM() != "NoProvided"){
      lshDF = lshDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.lshM()))
    }
    if(conf.lshS() != "NoProvided"){
      lshDF = lshDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.lshS()))
    }
    if(conf.flexmatcherM() != "NoProvided"){
      flexDF = flexDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.flexmatcherM()))
    }
    if(conf.flexmatcherS() != "NoProvided"){
      flexDF = flexDF.union(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(conf.flexmatcherS()))
    }

    run(spark, nextiaDF, flexDF, lshDF, conf.output().getAbsolutePath)

  }

}


class ConfEval(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("This code requires the files generates for the testbeds. You can find them at https://github.com/dtim-upc/spark/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/" +
    "\nThe followig options are required:\n\n")
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val nextiajd = opt[File](required = true, descr ="path to the discovery result file from nextiajd")
  val lsh = opt[File](required = true, descr ="path to the discovery result file from lsh")
  val flexmatcher = opt[File](required = true, descr ="path to the discovery result file from flexmatcher")
  val output = opt[File](required = true, descr ="path to write the results metrics e.g. confusion matrix")


  val nextiajdM = opt[String](required = false, descr ="path to the discovery file for testbed M from nextiajd", default=Some("NoProvided"))
  val lshM = opt[String](required = false, descr ="path to the discovery file for testbed M from lsh", default=Some("NoProvided"))
  val flexmatcherM = opt[String](required = false, descr ="path to the discovery file for testbed M from flexmatcher", default=Some("NoProvided"))

  val nextiajdS = opt[String](required = false, descr ="path to the discovery file for testbed S from nextiajd", default=Some("NoProvided"))
  val lshS = opt[String](required = false, descr ="path to the discovery file for testbed S from lsh", default=Some("NoProvided"))
  val flexmatcherS = opt[String](required = false, descr ="path to the discovery file for testbed S from flexmatcher", default=Some("NoProvided"))


  validateFileIsFile(nextiajd)
  validateFileIsFile(lsh)
  validateFileIsFile(flexmatcher)
  validateFileIsDirectory(output)
  verify()

}

