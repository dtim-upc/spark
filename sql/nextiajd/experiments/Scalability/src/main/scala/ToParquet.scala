import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.rogach.scallop.ScallopConf

object ToParquet {


  def readDataset(spark: SparkSession, pathDatasets: String, filename: String, delim: String, multiline: Boolean, nullVal: String, ignoreTrailing: Boolean): DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline", multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$pathDatasets/$filename")
  }

  def run(spark: SparkSession, pathInfo: String, pathDatasets: String, pathWrite: String): Unit = {
    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(pathInfo)
    // filename
    var lines: Seq[(String, String)] = Nil
    dsInfo.select("filename", "delimiter", "multiline", "nullVal", "ignoreTrailing").collect()
      .foreach {
        case Row(filename: String, delimiter: String, multiline: Boolean, nullVal: String, ignoreTrailing: Boolean) =>

          val ds = readDataset(spark, pathDatasets, filename, delimiter, multiline, nullVal, ignoreTrailing)

          val fn = filename.replace(".csv", "")

          val pathF = s"${pathWrite}/${fn}"

          var newDS = ds
          for (col <- ds.columns) {
            newDS = newDS.withColumnRenamed(col, col
              .replace(" ", "")
              .replace(",", "")
              .replace(";", "")
              .replace("{", "")
              .replace("}", "")
              .replace("(", "")
              .replace(")", "")
              .replace("\n", "")
              .replace("\t", "")
              .replace("=", "")
            )
          }

          newDS.write.mode(SaveMode.Overwrite).format("parquet").save(pathF)
          lines = lines :+ (fn, filename)
          println(s"Dataset ${filename} was wrote in ${pathF} ")

      }
    import spark.implicits._
    val dfInformation = lines.toDF("filename", "originalCSV")

    dfInformation.repartition(1).write.mode("overwrite").option("header", "true")
      .csv(s"${pathWrite}/tmp")

    val flag = getListOfFiles(new File(s"${pathWrite}/tmp"), List("csv"))
      .renameTo(new File(s"${pathWrite}/parquetFiles.csv"))

    if (flag) {
      println(s"File with datasets information is in  ${pathWrite}/parquetFiles.csv")
      FileUtils.deleteDirectory(new File(s"${pathWrite}/tmp"))
    } else {
      println("Cannot create file")
    }

  }

  def getListOfFiles(dir: File, extensions: List[String]): File = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }(0)
  }


  def main(args: Array[String]) {

    val conf = new ConfParquet(args)
    val spark = SparkSession.builder.appName("ToParquet Application").getOrCreate()

    run(spark, conf.datasetsInfo().getAbsolutePath,
      conf.pathDatasets().getAbsolutePath, conf.output().getAbsolutePath)
    spark.stop()

    //    import spark.implicits._
    //    val param = Seq("path to datasets information csv", "path to read datasets")
    //    if (args.size == 0) {
    //      println(s"Need arguments in order: ${param}")
    //      System.exit(0)
    //    }
    //
    //
    //    var cnt = 0
    //    for (p <- param) {
    //      if (cnt > args.size) {
    //        println(s"need parameter: ${p}")
    //        System.exit(0)
    //      }
    //      cnt = cnt + 1
    //    }
    //
    //    val pathDSInfo = args(0).toString;
    //    val pathDatasets = args(1).toString;
    //    val pathNewDSInfo = args(1).toString;
    //    run(spark, pathDSInfo, pathDatasets, pathNewDSInfo)
    //    spark.stop()
    //  }
  }
}



class ConfParquet(arguments: Seq[String]) extends ScallopConf(arguments) {
   footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val datasetsInfo = opt[File](required = true, descr ="path to datasets info file. This file contains the datasets names and their configuration to read them")
  val pathDatasets = opt[File](required = true, descr ="path to the folder where all datasets are")
  val output = opt[File](required = true, descr ="path to write results")


  validateFileIsFile(datasetsInfo)
  validateFileIsDirectory(pathDatasets)
  validateFileIsDirectory(output)
  verify()

}



