import java.io.File

import NextiaJD_evaluation.writeFile
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.NextiaJD.{second}

object SemanticNS {

  def assignClass(num0: Double, num1: Double, num2: Double, num3: Double,
                   num4: Double): Int = {
    var s = Seq((num4, 4), (num3, 3), (num2, 2), (num1, 1), (num0, 0))
    s.maxBy(_._1)._2
  }

  val rank = udf(assignClass(_: Double, _: Double, _: Double
    , _: Double, _: Double): Int)


  def main(args: Array[String]): Unit = {
    val conf = new ConfSemantic(args)

    val spark = SparkSession.builder().master("local[*]")
      .config("spark.driver.memory", "9g").config("spark.driver.maxResultSize", "9g")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val semantics = spark.read.option("header", "true").option("inferSchema","true")
      .csv(conf.semanticsNs().getAbsolutePath)


    val cvModel0 = CrossValidatorModel.load(s"${conf.modelsDir().getAbsolutePath}/class0")
    val cvModel1 = CrossValidatorModel.load(s"${conf.modelsDir().getAbsolutePath}/class1")
    val cvModel2 = CrossValidatorModel.load(s"${conf.modelsDir().getAbsolutePath}/class2")
    val cvModel3 = CrossValidatorModel.load(s"${conf.modelsDir().getAbsolutePath}/class3")
    val cvModel4 =CrossValidatorModel.load(s"${conf.modelsDir().getAbsolutePath}/class4")

    val dropCols = Seq("features", "rawPrediction", "probability", "prediction")

    val resultsM0 = cvModel0.transform(semantics).withColumn("p0", second(col("probability")))
          .drop(dropCols: _*)

    val resultsM1 = cvModel1.transform(resultsM0).withColumn("p1", second(col("probability")))
      .drop(dropCols: _*)

    val resultsM2 = cvModel2.transform(resultsM1).withColumn("p2", second(col("probability")))
      .drop(dropCols: _*)

    val resultsM3 = cvModel3.transform(resultsM2).withColumn("p3", second(col("probability")))
      .drop(dropCols: _*)

    val predAll = cvModel4.transform(resultsM3).withColumn("p4", second(col("probability")))
          .drop(dropCols: _*).select("ds_name", "att_name", "ds_name_2", "att_name_2", "p0", "p1",
          "p2", "p3", "p4", "flippedContainment", "bestContainment","trueQuality","cardinalityRaw","cardinalityRaw_2")
          .withColumn("prediction", rank(
            col("p0"), col("p1"), col("p2"), col("p3"), col("p4")
          ))

    val metrics = EvaluateDiscovery.getStats(spark, predAll, "Semantic non-syntactic")

    val metricsFile = conf.output()+s"/NextiaJD_semanticNS.txt"
    writeFile(metricsFile,Seq(metrics))

    println(metrics)

    println(s"\nWritting semantic non-syntactic metrics in file: ${metricsFile}")

    spark.stop()
  }
}


class ConfSemantic(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("This code requires the semanticsNS csv file. You can find more information at https://github.com/dtim-upc/spark/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/" +
    "\nThe followig options are required:\n\n")
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val semanticsNs = opt[File](required = true, descr ="path to datasets info file. This file contains the datasets names and their configuration to read them")
  val modelsDir = opt[File](required = true, descr ="path to the folder with the models")
  val output = opt[File](required = true, descr ="path to write the discovery and time results")

  validateFileIsFile(semanticsNs)
  validateFileIsDirectory(modelsDir)
  validateFileIsDirectory(output)
  verify()

}


