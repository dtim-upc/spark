import java.io._

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lower, trim}
import org.apache.spark.sql.types.StringType
import org.rogach.scallop.ScallopConf
import utils.AttComparison

import scala.collection.mutable

object GroundTruth {


  val spark = SparkSession.builder.appName("SparkSQL")
    .master("local[*]")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.maxResultSize", "4g")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  def readDataset(pathDatasets: String, filename: String, delim: String, multiline: Boolean, ignoreTrailing: Boolean, fileType: String): DataFrame = {

    if ( fileType.toLowerCase() == "csv") {

      println(s"Reading filename ${filename} with parameters delim ${delim.trim}, multiline ${multiline},  trailing ${ignoreTrailing}")

      val dataset = spark.read
        .option("header", "true").option("inferSchema", "true")
        .option("delimiter", delim.trim).option("quote", "\"")
        .option("escape", "\"").option("multiline",multiline)
        //      .option("nullValue", nullVal)
        .option("ignoreTrailingWhiteSpace", ignoreTrailing)
        .csv(s"$pathDatasets/$filename")

      dataset
    } else {
      println(s"Reading parquet ${filename}")

      val dataset = spark.read.parquet(s"$pathDatasets/$filename")
//      dataset.show()
      dataset
    }

  }


  def getDistinctValues(pathSets: String, fileName: String, attName: String, df: DataFrame): (DataFrame, String) = {
    val cleanAttName = attName.replace(" ","")
    try {
      // try to read the parquet file with the distinct values. If file does not exist,
      // produces exception and we compute the distinct values for the attribute.
      (spark.read.parquet(s"${pathSets}/${fileName}.${cleanAttName}"), cleanAttName)
    } catch {
      case e:
        AnalysisException =>
//        println(s"Couldn't read the set of distinct values for file $fileName and attribute $attName")

        // convert all values to lowecase and trim values
        val distinctDF = df.select(lower(trim(col(attName))).as(cleanAttName)).na.drop.distinct()

        writeDistinctValues(distinctDF, fileName, cleanAttName, pathSets)
        (distinctDF, cleanAttName)
    }
  }


  def compareAtt(pathSets:String, f1: String, ds1:DataFrame, f2: String, ds2: DataFrame): mutable.MutableList[AttComparison] = {

    val attNames1 = ds1.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
    val attNames2 = ds2.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)

    if ( attNames1.size == 0) {
      println(s"Dataset ${f1} has 0 string attributes")
    }

    if ( attNames2.size == 0) {
      println(s"Dataset ${f2} has 0 string attributes")
    }

    //    var lines: Seq[(String, String, String, String,String, String, String, String )] = Nil
    val comparisonList = mutable.MutableList[AttComparison]()

    for( att1 <- attNames1 )
      for( att2 <- attNames2 ){


        val (d1, cleanAtt1) = getDistinctValues(pathSets, f1, att1, ds1 )
        val (d2, cleanAtt2) = getDistinctValues(pathSets, f2, att2, ds2 )

        val size1 = d1.count().toDouble
        val size2 = d2.count().toDouble

        val joinExpression = d1.col( cleanAtt1 ) === d2.col( cleanAtt2 )
        val j = d1.join(d2,joinExpression)
        val tuplas = j.count().toDouble

        val containment1 = containment(tuplas, size1)
        val containment2 = containment(tuplas, size2)


        val AB = AttComparison(f1, att1, size1, f2, att2, size2, tuplas, containment1 )
        val BA = AttComparison(f2, att2, size2, f1, att1, size1, tuplas, containment2 )

        comparisonList += AB
        comparisonList += BA

      }
    comparisonList
  }

  def containment( overlapping: Double , distinctValues: Double   ): Double = {
    overlapping/distinctValues
  }

  def writeDistinctValues(ds: DataFrame, filename:String ,att: String, pathSets: String) = {
    val name = s"${filename}.${att}"
    ds.write.parquet(s"${pathSets}/${name}")
  }


  def compute(pathDatasetInfo: String, pathDatasets: String ,pathSets: String, pathOutput: String) = {


    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(pathDatasetInfo)

    // mapDS contains as key the filename and as value the respective dataframe
    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename","delimiter", "multiline","ignoreTrailing", "fileType").collect()
      .foreach{
          case Row( filename: String, delimiter: String, multiline:Boolean,  ignoreTrailing: Boolean, fileType:String) =>
            mapDS = mapDS + (filename -> readDataset(pathDatasets, filename,delimiter,multiline,ignoreTrailing, fileType))
        case Row(filename:String,  null, null,  null, fileType:String) =>
          mapDS = mapDS + (filename -> readDataset(pathDatasets, filename,",",false,false, fileType))
      }

    val listFiles = mapDS.keySet.toSeq
    val size = listFiles.size
    println(s"The number of datasets read are: $size")

    val t1 = System.nanoTime

    val comparison = mutable.MutableList[AttComparison]()
//    var comparison: Seq[AttComparison] = Nil

    for(i <- 0 to size-2){
      for(j <- i+1 to size-1){
        val fileName1 = listFiles(i)
        val fileName2 = listFiles(j)

        println(s"Computing ground truth for datasets: $fileName1 - $fileName2")
        val ds1 = mapDS.get(fileName1).get
        val ds2 =mapDS.get(fileName2).get

        comparison ++= compareAtt(pathSets, fileName1,ds1,fileName2,ds2)

      }
    }

    val personDF = comparison.toDF()
    personDF.repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${pathOutput}/groundtruth")


  }


  def stop() :Unit = {
    spark.stop()
  }

  def test() = {
    val ds = readDataset("/Users/javierflores/Documents/GroungTruthJoins/datasets/raw","a.csv",",",false,false,"csv")
    ds.repartition(1).write.mode("overwrite").option("header","true")
      .parquet(s"/Users/javierflores/Documents/GroungTruthJoins/datasets/output/parquet")


  }

  def main(args: Array[String]): Unit = {


    val conf = new Conf(args)
//
//    val pathDatasetInfo = "/Users/javierflores/Documents/GroungTruthJoins/datasets/datasetInfo.csv"
////      s"/Users/javierflores/Documents/Research/Projects/FJA/Benchmark/datasetInformation_benchmark.csv"
//    val pathGroundTruth = ""
//    val pathDatasets = "/Users/javierflores/Documents/GroungTruthJoins/datasets/raw"
//    val pathSets = "/Users/javierflores/Documents/GroungTruthJoins/datasets/sets"
//    val pathOutput = "/Users/javierflores/Documents/GroungTruthJoins/datasets/output"

    compute(conf.datasetsInfo().getAbsolutePath, conf.pathDatasets().getAbsolutePath,
      conf.sets().getAbsolutePath, conf.output().getAbsolutePath )
//
    stop()
//    test()

  }
}


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("The followig options are required:\n")
//  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")
//
  val datasetsInfo = opt[File](required = true, descr ="path to datasets info file. This file contains the datasets names and their configuration to read them")

  val pathDatasets = opt[File](required = true, descr ="path to the folder where the source datasets are")
  val sets = opt[File](required = true, descr ="path to save the sets of distinct values for each attribute")
  val output = opt[File](required = true, descr ="path to write the discovery and time results")


  validateFileIsFile(datasetsInfo)
  validateFileIsDirectory(sets)
  validateFileIsDirectory(pathDatasets)
  validateFileIsDirectory(output)
  verify()

}