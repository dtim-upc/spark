import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.rogach.scallop.ScallopConf


object EvaluateDiscovery {

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
    lines = lines + "\n"
    lines = lines + "Confusion matrix:\n"
    lines = lines +  mMetrics.confusionMatrix.toString()
    lines = lines + "\n"
    lines
  }

  def run(spark: SparkSession, nextia: String, flex: String, lsh: String, output: String): Unit = {

    val nextiaDF = spark.read.option("header", "true").option("inferSchema", "true").csv(nextia)
      .withColumn("prediction",
        when(col("quality") === "High", lit(4D))
          .when(col("quality") === "Good", lit(3D))
          .when(col("quality") === "Moderate", lit(2D))
          .when(col("quality") === "Poor", lit(1D))
          .otherwise(lit(0D))
      )


    var lines = getStats(spark, nextiaDF, "NextiaJD")

    val flexDF = spark.read.option("header", "true").option("inferSchema", "true").csv(flex)
      .withColumn("prediction", lit(1))

    val dsFlex = nextiaDF.select("query dataset","query attribute","candidate dataset","candidate attribute","trueQuality")
      .join(flexDF,Seq("query dataset","query attribute","candidate dataset","candidate attribute"),"left_outer").na.fill(0)
      .withColumn("trueQuality", when(col("trueQuality") === 0,0).otherwise(1))

    lines = lines + getStats(spark, dsFlex, "FlexMatcher")

    val lshDF = spark.read.option("header", "true").option("inferSchema", "true").csv(lsh)


    val dsLSH = nextiaDF.select("sizeDistinct1","sizeDistinct2","query dataset","query attribute","candidate dataset","candidate attribute","trueQuality","trueContainment")
      .join(lshDF,Seq("query dataset","query attribute","candidate dataset","candidate attribute"),"left_outer")

    dsLSH.printSchema()
    val tmp = dsLSH.withColumn("prediction", setQuality(col("trueContainment"),col("sizeDistinct1"), col("sizeDistinct2")))
      .groupBy("query dataset","query attribute","candidate dataset","candidate attribute", "trueQuality","trueContainment","sizeDistinct1","sizeDistinct2")
      .agg(org.apache.spark.sql.functions.max("threshold").as("threshold"))
      .na.fill(0).withColumn("prediction", setQuality(col("threshold"),col("sizeDistinct1"), col("sizeDistinct2")))


    lines = lines + getStats(spark, tmp, "LSH Ensemble")

    writeFile(output+s"/Comparison_state_of_the_art.txt", Seq(lines))

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

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.memory", "9g")
      .config("spark.driver.maxResultSize", "9g")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    run(spark, conf.nextiajd().getAbsolutePath, conf.flexmatcher().getAbsolutePath
      ,conf.lsh().getAbsolutePath, conf.output().getAbsolutePath)

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

//  query dataset,query attribute,candidate dataset,candidate attribute,sizeDistinct1,sizeDistinct2,joinSize,trueContainment,trueQuality,quality,probability

  validateFileIsFile(nextiajd)
  validateFileIsFile(lsh)
  validateFileIsFile(flexmatcher)
  validateFileIsDirectory(output)
  verify()

}

