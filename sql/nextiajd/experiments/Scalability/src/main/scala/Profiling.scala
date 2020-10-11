import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopConf

object Profiling {

 var mapReading:Map[String, Double] = Map()
  var mapProfiling:Map[String, Double] = Map()

  var linesTime:Seq[String] = Nil

  def readDataset(spark: SparkSession, pathDatasets:String, filename: String,
                  delim : String, multiline: Boolean,nullVal: String,
                  ignoreTrailing: Boolean):DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline",multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$pathDatasets/$filename")
  }

  def readParquet(spark: SparkSession, pathDatasets:String, filename: String): DataFrame ={
    spark.read.load(s"$pathDatasets/$filename")
  }

  def runParquet(spark: SparkSession,pathInfo: String, pathDatasets:String, pathWriteTimes: String, overrideFlag: Boolean): Unit ={
    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(pathInfo)

    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename").collect()
      .foreach{
        case Row( filename: String) =>

          val timeR = System.nanoTime
          mapDS = mapDS + (filename -> readParquet(spark, pathDatasets, filename))
          val endtimer = (System.nanoTime - timeR).abs / 6e10
          println(s"Time reading parquet ${filename} : ${endtimer} minutes")
          mapReading += filename -> endtimer
      }

    val listFiles = mapDS.keySet.toSeq


    for(f <- listFiles){
      println(s"profiling dataset ${f}")
      val timeProfiling = System.nanoTime
      //      mapDS.get(f).get.metaFeatures
      mapDS.get(f).get.attributeProfile(overrideFlag)
      val endProfiling = (System.nanoTime - timeProfiling).abs / 6e10
            print(s"profiling time: ${endProfiling} minutes")
      mapProfiling += f -> endProfiling
    }

    val totalTimeReading = mapReading.values.foldLeft(0.0)(_+_)
    val totalTimeProfiling = mapProfiling.values.foldLeft(0.0)(_+_)


    val s1 = s"\n\nTotal time reading: ${totalTimeReading}" +
      s"\nAverage time reading: ${totalTimeReading/listFiles.size}" +
      s"\nTotal time profiling: ${totalTimeProfiling}" +
      s"\nAverage time profiling: ${totalTimeProfiling/listFiles.size}" +
      s"\nTime pre: ${(totalTimeReading+totalTimeProfiling)/listFiles.size}"+
      s"\n\ntimes reading: ${mapReading.toString}\n" +
      s"\n\ntimes profiling: ${mapProfiling.toString}\n"

    println(s1)


    writeFile(s"${pathWriteTimes}",Seq(s1))


  }


  def run(spark: SparkSession,pathInfo: String, pathDatasets:String, pathWriteTimes: String, overrideFlag: Boolean): Unit ={
    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(pathInfo)

    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename","delimiter", "multiline","nullVal","ignoreTrailing").collect()
      .foreach{
        case Row( filename: String, delimiter: String, multiline:Boolean, nullVal: String, ignoreTrailing: Boolean) =>

          val timeR = System.nanoTime
          mapDS = mapDS + (filename -> readDataset(spark, pathDatasets, filename,delimiter,multiline,nullVal,ignoreTrailing))
          val endtimer = (System.nanoTime - timeR).abs / 6e10
          println(s"${filename} : ${endtimer}")
          mapReading += filename -> endtimer
      }

    val listFiles = mapDS.keySet.toSeq


    for(f <- listFiles){
      println(s"profiling dataset ${f}")
      val timeProfiling = System.nanoTime
//      mapDS.get(f).get.metaFeatures
      mapDS.get(f).get.attributeProfile(overrideFlag)
      val endProfiling = (System.nanoTime - timeProfiling) / 6e10
      print(s"profiling time: ${endProfiling} minutes")
      mapProfiling += f -> endProfiling
    }

    val totalTimeReading = mapReading.values.foldLeft(0.0)(_+_)
    val totalTimeProfiling = mapProfiling.values.foldLeft(0.0)(_+_)

    val lineDivider = "\n---------------------------------------------------------\n"
    println(lineDivider)

    val s1 = s"\n\nTotal time reading: ${totalTimeReading} minutes" +
      s"\nAverage time reading: ${totalTimeReading/listFiles.size} minutes" +
      s"\nTotal time profiling: ${totalTimeProfiling} minutes" +
      s"\nAverage time profiling: ${totalTimeProfiling/listFiles.size} minutes" +
      s"\nTotal time pre runtime: ${(totalTimeReading+totalTimeProfiling)} minutes"+
      s"\n\ntimes reading: ${mapReading.toString}\n" +
      s"\n\ntimes profiling: ${mapProfiling.toString}\n"

    println(s1)


    writeFile(s"${pathWriteTimes}",Seq(s1))


  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)
    val spark = SparkSession.builder.appName("Profiling Application_2").getOrCreate()

    if( conf.typeProfile() == "csv" ) {
      run(spark, conf.datasetsInfo().getAbsolutePath, conf.pathDatasets().getAbsolutePath,
        s"${conf.output().getAbsolutePath}/times.txt" , true)
    } else {
      runParquet(spark, conf.datasetsInfo().getAbsolutePath, conf.pathDatasets().getAbsolutePath,
        s"${conf.output().getAbsolutePath}/times.txt" , true)
    }
  }


}



class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val typeProfile =  opt[String](required = true, descr ="The type of files for profiling. It can be csv or parquet. Default is csv", default = Some("csv") )
  val datasetsInfo = opt[File](required = true, descr ="path to datasets info file. This file contains the datasets names and their configuration to read them")
  val pathDatasets = opt[File](required = true, descr ="path to the folder where all datasets are")
  val output = opt[File](required = true, descr ="path to write results")


  validateFileIsFile(datasetsInfo)
  validateFileIsDirectory(pathDatasets)
  validateFileIsDirectory(output)
  verify()

}
