import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StringType
import org.rogach.scallop.ScallopConf

object GeneratorN {


  def run(spark: SparkSession, pathDataset: String, pathWriteDataset: String,
          numberLinesN: Int, delim: String, multiline: Boolean, nullVal: String,
          ignoreTrailing: Boolean, hadoopEnable: Boolean = false): Unit = {

    //    val originalDS = spark.read.option("header", "true").option("inferSchema", "true").csv(pathDataset)
    val originalDS = spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline", multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$pathDataset")
    var newDS = originalDS
    val actualNumberColumns = originalDS.schema.size

    val StringColumns = originalDS.schema.filter(_.dataType == StringType).map(_.name)
    val otherColumns = originalDS.schema.filter(_.dataType != StringType).map(_.name)


    var cnt = 0
    var index = 0
    val lines = newDS.count()
    while (cnt <= numberLinesN) {

      if (numberLinesN < lines) {
        newDS = newDS.union(newDS.limit(numberLinesN))
        cnt = cnt + numberLinesN + 1
      } else {
        newDS = newDS.union(newDS)
        cnt = cnt + lines.toInt + 1
      }

    }

    newDS.repartition(1).write.mode("overwrite").option("header", "true")
      .csv(s"${pathWriteDataset}/tmp")

    val filename = originalDS.inputFiles(0).split("/").last.replace(".csv", "")
    val pathDirFile = s"${pathWriteDataset}/${filename}_genN${numberLinesN}.csv"
    val tmpDir = s"${pathWriteDataset}/tmp"


    val flag = getListOfFiles(new File(tmpDir), List("csv"))
      .renameTo(new File(pathDirFile))

    if (flag) {
      print(s"New file created ${pathDirFile}")
      FileUtils.deleteDirectory(new File(tmpDir))
    } else {
      print(s"Cannot move file. File was created in ${tmpDir}")
    }


  }


  def getListOfFiles(dir: File, extensions: List[String]): File = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }(0)
  }

  def main(args: Array[String]) {
    val conf = new ConfGenN(args)
    val spark = SparkSession.builder.appName("GeneratorM Application").getOrCreate()

    run(spark, conf.pathDataset().getAbsolutePath, conf.output().getAbsolutePath,
      conf.n(), conf.delimiter(), conf.multiline(), conf.nullval(), conf.ignoreTrailing())

    spark.stop()
    //    println("parameters size: " + args.size)
    //    if (args.size >= 3) {
    //
    //      val pathDataset = args(0).toString;
    //      val pathWriteDataset = args(1).toString;
    //      val numberLinesN = args(2).toInt;
    //
    //      var delim = ","
    //      var multiline = false
    //      var nullVal = ""
    //      var ignoreT = true
    //      var hadoopEnable = false
    //      if (args.size > 3) {
    //        delim = args(3);
    //        multiline = args(4).toBoolean;
    //        nullVal = args(5)
    //        ignoreT = args(6).toBoolean
    //      }
    //
    //      if (args.size == 8) {
    //        hadoopEnable = args(7).toBoolean
    //      }
    //
    //      run(spark, pathDataset, pathWriteDataset, numberLinesN, delim, multiline, nullVal, ignoreT)
    //
    //    } else {
    //      println("**Need parameters in the following order: path for datasets information, path for datasets and path for write times")
    //    }
    //
    //    spark.stop()
    //  }
  }

}



class ConfGenN(arguments: Seq[String]) extends ScallopConf(arguments) {
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val pathDataset = opt[File](required = true, descr ="path to the dataset")
  val output = opt[File](required = true, descr ="path to write the new dataset")
  val n = opt[Int](required = true, descr ="Number of N rows to replicate")
  val delimiter = opt[String](required = true, descr ="Dataset delimiter. Default value is ,", default = Some(","))
  val multiline = opt[Boolean](required = true, descr ="Indicate if dataset is multiline. Default value is false", default = Some(false))
  val nullval = opt[String](required = true, descr ="Dataset null value. Default value is \"\"", default = Some("\"\""))
  val ignoreTrailing = opt[Boolean](required = true, descr ="Ignores dataset trailing. Default value is true", default = Some(true))

  validateFileIsFile(pathDataset)
  validateFileIsDirectory(output)
  verify()

}

