import java.io._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.NextiaJD
import org.apache.spark.sql.types.StringType
import org.rogach.scallop.ScallopConf

object NextiaJD_evaluation {

  var linesTime:Seq[String] = Nil

  var mapReading:Map[String, Double] = Map()
  var mapProfiling:Map[String, Double] = Map()
  var mapQuerying:Map[String, Double] = Map()

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.memory", "9g")
    .config("spark.driver.maxResultSize", "9g")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  def readDataset(path: String, delim : String, multiline: Boolean,nullVal: String, ignoreTrailing: Boolean):DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline",multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$path")
  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

  def run(datasetsInfo: String, datasetsDir: String, output: String,
          testbed: String, ground: String, queryType: String): Unit = {

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(datasetsInfo)

    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename","delimiter", "multiline","nullVal","ignoreTrailing").collect()
      .foreach{
        case Row( filename: String, delimiter: String, multiline:Boolean, nullVal: String, ignoreTrailing: Boolean) =>

          val timeR = System.nanoTime
          mapDS = mapDS + (filename -> readDataset(s"${datasetsDir}/${filename}",delimiter,multiline,nullVal,ignoreTrailing))
          val endtimer = (System.nanoTime - timeR).abs / 6e10
          mapReading += filename -> endtimer

      }

    println(s"Testbed ${testbed} has ${mapDS.keySet.size} datasets")
    val listFiles = mapDS.keySet.toSeq

    // computing profiling
    println("start profiling")

    for(f <- listFiles){
      println(s"profiling dataset ${f}")
      val timeProfiling = System.nanoTime
      mapDS.get(f).get.attributeProfile()
      val endProfiling = (System.nanoTime - timeProfiling).abs / 6e10
      println(s"profiling time: ${endProfiling} minutes")
      mapProfiling += f -> endProfiling
    }

    val totalTimeReading = mapReading.values.foldLeft(0.0)(_+_)
    val totalTimeProfiling = mapProfiling.values.foldLeft(0.0)(_+_)

    println("start discovery")
    val totalTimeQuerying = System.nanoTime
    var cnt = 0
    var cnt2 = 0
    for(queryDataset <- listFiles){

      cnt = cnt +1
      val qD = mapDS.get(queryDataset).get

      if(queryType == "querybydataset"){
        println(s"Discovery for $queryDataset")
        val timeQuerying = System.nanoTime
        val discovery = NextiaJD.discovery(qD, mapDS.-(queryDataset).values.toSeq, showAll = true)
        discovery.repartition(1).write.mode("overwrite").option("header","true")
          .csv(s"${output}/discovery${cnt}.csv")

        val dQuerying = (System.nanoTime - timeQuerying).abs / 6e10
        mapQuerying += queryDataset -> dQuerying
        println(s"${queryDataset} discovery time: $dQuerying minutes")
      } else {
        val queryAtts = qD.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
        for (qA <- queryAtts) {
          cnt2 = cnt2 + 1
          println(s"Discovery for $queryDataset for attribute $qA")
          val timeQuerying = System.nanoTime
          val discovery = NextiaJD.discovery(qD, mapDS.-(queryDataset).values.toSeq, qA, showAll = true)
          discovery.repartition(1).write.mode("overwrite").option("header","true")
            .csv(s"${output}/discovery${cnt2}.csv")

          val dQuerying = (System.nanoTime - timeQuerying).abs / 6e10
          mapQuerying += s"${queryDataset}:${qA}" -> dQuerying
          println(s"discovery time for queryAtt ${qA} : $dQuerying minutes")
        }
      }


    }
    val durationQuering = (System.nanoTime - totalTimeQuerying).abs / 6e10
    println(s"execution time:  ${durationQuering} minutes")

    val totalTQ = mapQuerying.values.foldLeft(0.0)(_+_)

    val lineDivider = "---------------------------------------------------------\n"
    val linePre = s"Total time pre runtime: ${(totalTimeReading+totalTimeProfiling)} minutes\n"
    val lineRuntime = s"Total time runtime: ${totalTQ} minutes \n"
    val lineAtt = if(queryType == "querybydataset") s"Datasets for querying: ${mapQuerying.size}\n" else s"Attributes for querying: ${mapQuerying.size}\n"

    linesTime = linesTime :+ lineDivider :+ linePre :+ lineRuntime :+ lineAtt :+ lineDivider

    val otherTimes = s"\n\nTotal time reading: ${totalTimeReading} minutes" +
      s"\nAverage time reading: ${totalTimeReading/listFiles.size} minutes" +
      s"\nTotal time profiling: ${totalTimeProfiling} minutes" +
      s"\nAverage time profiling: ${totalTimeProfiling/listFiles.size} minutes" +
      s"\nAverage time runtime: ${totalTQ/mapQuerying.size} minutes" +
      s"\nAverage time pre runtime: ${(totalTimeReading+totalTimeProfiling) / listFiles.size} minutes" +
      s"\n\ntimes reading: ${mapReading.toString}\n" +
      s"\n\ntimes profiling: ${mapProfiling.toString}\n" +
      s"\n\ntimes querying: ${mapQuerying.toString}\n"
    linesTime = linesTime :+ otherTimes
    println(linesTime.toString)

    writeFile(output+s"/time_testbed${testbed}.txt",linesTime)

    uniteDiscoveries(mapDS.size, output, testbed, ground)
  }



  def uniteDiscoveries(datasetsNumber: Int, output: String, testbed: String, ground: String): Unit = {

    var mapDS: Seq[DataFrame] = Nil
    for (i <- 1 to datasetsNumber) {
      val folder = s"discovery${i}.csv"
      mapDS = mapDS :+ spark.read.option("header", "true").option("inferSchema", "true")
        .csv(s"${output}/${folder}/*.csv")
    }

    var ds = mapDS(0)
    for (cnt <- 0 to mapDS.size-1) {
      if(cnt > 1) {
        ds = ds.union(mapDS(cnt))
      }
    }

    // join discovery with ground truth
    val groundDF = spark.read.option("header", "true").option("inferSchema", "true").csv(ground)
      .withColumnRenamed("ds_name", "query dataset")
      .withColumnRenamed("ds_name_2", "candidate dataset")
      .withColumnRenamed("att_name", "query attribute")
      .withColumnRenamed("att_name_2", "candidate attribute")
//      .select("query dataset", "query attribute", "candidate dataset", "candidate attribute", "trueContainment", "trueQuality")

    ds = groundDF.join(ds, Seq("query dataset", "query attribute", "candidate dataset", "candidate attribute"))

    val tmpDir = s"${output}/tmp"
    ds.repartition(1).write.mode("overwrite").option("header","true")
      .csv(tmpDir)

    val tempDF =  ds.withColumn("prediction",
      when(col("quality") === "High", lit(4D))
        .when(col("quality") === "Good", lit(3D))
        .when(col("quality") === "Moderate", lit(2D))
        .when(col("quality") === "Poor", lit(1D))
        .otherwise(lit(0D))
    )

    val metrics = EvaluateDiscovery.getStats(spark, tempDF, "NextiaJD")
    val metricsFile = output+s"/NextiaJD_evaluation_testbed${testbed}.txt"
    writeFile(metricsFile,Seq(metrics))
    println(s"Writting metrics in file: ${metricsFile}")


    val pathW = s"${output}/nextiaJD_testbed${testbed}.csv"

    val flag = getListOfFiles(new File(tmpDir), List("csv"))
      .renameTo(new File(pathW))

    if (flag) {
      print(s"Writting discovery in file: ${pathW}")
      FileUtils.deleteDirectory(new File(tmpDir))

      for (i <- 1 to datasetsNumber) {
        val folder = s"discovery${i}.csv"
        FileUtils.deleteDirectory(new File(s"${output}/${folder}"))

      }

    } else {
      print(s"Cannot move file. File was created in ${tmpDir}")
    }





  }

  def getListOfFiles(dir: File, extensions: List[String]): File = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }(0)
  }

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)

    run(conf.datasetsInfo().getAbsolutePath, conf.pathDatasets().getAbsolutePath,
      conf.output().getAbsolutePath, conf.testbed(), conf.groudTruth().getAbsolutePath, conf.queryType())

    stop()
  }

  def stop() :Unit = {
    spark.stop()
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("This code requires the files generates for the testbeds. You can find them at https://github.com/dtim-upc/spark/tree/nextiajd_v3.0.1/sql/nextiajd/experiments/" +
    "\nThe followig options are required:\n\n")
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val datasetsInfo = opt[File](required = true, descr ="path to datasets info file. This file contains the datasets names and their configuration to read them")

  val pathDatasets = opt[File](required = true, descr ="path to the folder where all datasets are")
  val groudTruth = opt[File](required = true, descr ="path to the ground truth csv file")
  val output = opt[File](required = true, descr ="path to write the discovery and time results")
  val testbed = opt[String](required = false, descr ="testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated", default=Some(""))
  val queryType = opt[String](required = false, validate = Seq("querybydataset","querybyattribute").contains(_) ,descr ="the query search. There are two types: querybydataset and querybyattribute. Default value is querybydataset.", default = Some("querybydataset"))

  validateFileIsFile(groudTruth)
  validateFileIsFile(datasetsInfo)
  validateFileIsDirectory(pathDatasets)
  validateFileIsDirectory(output)
  verify()

}
