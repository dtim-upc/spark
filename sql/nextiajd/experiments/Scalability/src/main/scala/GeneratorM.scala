import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopConf

object GeneratorM {


  def run(spark: SparkSession, pathDataset: String, pathWriteDataset: String, numberColumns: Int,
          delim: String, multiline: Boolean, nullVal: String,
          ignoreTrailing: Boolean, hadoopEnable: Boolean = false): Unit = {

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
    while (cnt < numberColumns) {

      if (index > StringColumns.size - 1) {
        index = 0
      }

      val colname = StringColumns(index)
      newDS = newDS.withColumn(s"${colname}${cnt}", col(colname))

      cnt = cnt + 1
      index = index + 1

    }


    val filename = originalDS.inputFiles(0).split("/").last.replace(".csv", "")
    val pathDirFile = s"${pathWriteDataset}/${filename}_genM${numberColumns}.csv"
    val tmpDir = s"${pathWriteDataset}/tmp"

      newDS.repartition(1).write.mode("overwrite").option("header", "true")
        .csv(s"${pathWriteDataset}/tmp")
      val flag = getListOfFiles(new File(tmpDir), List("csv"))
        .renameTo(new File(pathDirFile))

      if (flag) {
        print(s"New file created ${pathDirFile}")
        FileUtils.deleteDirectory(new File(tmpDir))
      } else {
        print("Cannot create file")
      }




  }


  def getListOfFiles(dir: File, extensions: List[String]): File = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }(0)
  }

  def main(args: Array[String]) {
    val conf = new ConfGenM(args)
    val spark = SparkSession.builder.appName("GeneratorM Application").getOrCreate()
    run(spark, conf.pathDataset().getAbsolutePath, conf.output().getAbsolutePath,
      conf.m(), conf.delimiter(), conf.multiline(), conf.nullval(), conf.ignoreTrailing())

  }


}



class ConfGenM(arguments: Seq[String]) extends ScallopConf(arguments) {
  footer("\nFor more information, consult the documentation at https://github.com/dtim-upc/spark")

  val pathDataset = opt[File](required = true, descr ="path to the dataset")
  val output = opt[File](required = true, descr ="path to write the new dataset")
  val m = opt[Int](required = true, descr ="Number of M columns to replicate")
  val delimiter = opt[String](required = true, descr ="Dataset delimiter. Default value is ,", default = Some(","))
  val multiline = opt[Boolean](required = true, descr ="Indicate if dataset is multiline. Default value is false", default = Some(false))
  val nullval = opt[String](required = true, descr ="Dataset null value. Default value is \"\"", default = Some("\"\""))
  val ignoreTrailing = opt[Boolean](required = true, descr ="Ignores dataset trailing. Default value is true", default = Some(true))

  validateFileIsFile(pathDataset)
  validateFileIsDirectory(output)
  verify()

}
