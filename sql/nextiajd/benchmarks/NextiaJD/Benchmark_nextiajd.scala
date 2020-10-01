import java.io._

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.NextiaJD

object Benchmark_nextiajd {

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

  val category = 2
  val path =s"/Users/javierflores/Documents/Research/Projects/FJA/Benchmark/datasets2/"
  val pathWriteDiscovery = s"/Users/javierflores/Documents/Research/Projects/FJA/Scripts/resources/newModels/${category}"
  val pathWriteTimes = s"/Users/javierflores/Documents/Research/Projects/FJA/Scripts/resources/newModels/times/times${category}_prueba4_1.txt"

  def readDataset(filename: String, delim : String, multiline: Boolean,nullVal: String, ignoreTrailing: Boolean):DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("delimiter", delim).option("quote", "\"")
      .option("escape", "\"").option("multiline",multiline)
      .option("nullValue", nullVal)
      .option("ignoreTrailingWhiteSpace", ignoreTrailing)
      .csv(s"$path/$filename")
  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

  def run(): Unit = {

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val dsInfo = spark.read.option("header", "true").option("inferSchema", "true").
      csv(s"/Users/javierflores/Documents/Research/Projects/FJA/Benchmark/datasetInformation_benchmark.csv")

    var mapDS = Map[String, DataFrame]()
    dsInfo.filter(col("category") === category).select("filename","delimiter", "multiline","nullVal","ignoreTrailing").collect()
      .foreach{
        case Row( filename: String, delimiter: String, multiline:Boolean, nullVal: String, ignoreTrailing: Boolean) =>

          if (filename != "VOTER_Survey_December16_Release1.csv" && filename != "survey_results_public.csv"){

            val timeR = System.nanoTime
            mapDS = mapDS + (filename -> readDataset(filename,delimiter,multiline,nullVal,ignoreTrailing))
            val endtimer = (System.nanoTime - timeR) / 1e9d
            mapReading += filename -> endtimer/60
          } else {
            println("exclude voter_survey..")
          }


      }

    println(s"Category ${category} has ${mapDS.keySet.size} datasets")
    val listFiles = mapDS.keySet.toSeq

    // computing profiling
    println("start profiling")

    for(f <- listFiles){
      println(s"profiling dataset ${f}")
      val timeProfiling = System.nanoTime
      mapDS.get(f).get.metaFeatures
      val endProfiling = (System.nanoTime - timeProfiling) / 1e9d
      println(s"profiling time: ${endProfiling}")
      mapProfiling += f -> endProfiling/60
    }

    val totalTimeReading = mapReading.values.foldLeft(0.0)(_+_)
    val totalTimeProfiling = mapProfiling.values.foldLeft(0.0)(_+_)

    val s1 = s"\nTotal time reading: ${totalTimeReading}" +
      s"\nAverage time reading: ${totalTimeReading/listFiles.size}" +
      s"\nTotal time profiling: ${totalTimeProfiling}" +
      s"\nAverage time profiling: ${totalTimeProfiling/listFiles.size}" +
      s"\nTime pre: ${(totalTimeReading+totalTimeProfiling)/listFiles.size}"+
      s"\n\ntimes reading: ${mapReading.toString}\n" +
      s"\ntimes profiling: ${mapProfiling.toString}\n"

    linesTime = linesTime :+ s1


    println(s1)
    println("start discovery")
    val totalTimeQuerying = System.nanoTime
    var cnt = 0
    for(queryDataset <- listFiles){

      cnt = cnt +1
      val qD = mapDS.get(queryDataset).get

//      val queryAtts = qD.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)

      println(s"Discovery for $queryDataset")
      val timeQuerying = System.nanoTime
      val discovery = NextiaJD.discovery(qD, mapDS.-(queryDataset).values.toSeq)
      discovery.repartition(1).write.mode("overwrite").option("header","true")
        .csv(s"${pathWriteDiscovery}/discovery${cnt}.csv")

      val dQuerying = (System.nanoTime - timeQuerying) / 1e9d
      mapQuerying += queryDataset -> dQuerying/60
      println(s"${queryDataset} discovery time: $dQuerying")




    }
    val durationQuering = (System.nanoTime - totalTimeQuerying) / 1e9d
    println(s"execution time:  ${durationQuering} seconds")

    val totalTQ = mapQuerying.values.foldLeft(0.0)(_+_)

    val s11 = s"Total time querying: ${totalTQ}\n"
    val s22 = s"Average time querying: ${totalTQ/mapQuerying.size}\n"
    val s33 = s"times querying: ${mapQuerying.toString}\n"
    val s44 = s"Attributes for querying: ${mapQuerying.size}\n"

    linesTime = linesTime :+ s11
    linesTime = linesTime :+ s22
    linesTime = linesTime :+ s33
    linesTime = linesTime :+ s44


    println(s"${s22}\n${s33}\n${s44}")

    writeFile(pathWriteTimes,linesTime)

    uniteDiscoveries(mapDS.size)
  }


  def uniteDiscoveries(datasetsNumber: Int): Unit = {

    var mapDS: Seq[DataFrame] = Nil
    for (i <- 1 to datasetsNumber) {

      println(s"reading discovery ${i}")
      val folder = s"discovery${i}.csv"
      mapDS = mapDS :+ spark.read.option("header", "true").option("inferSchema", "true")
        .csv(s"${pathWriteDiscovery}/${folder}/*.csv")


    }
    println(s"unite ${mapDS.size} discoveries")
    var ds = mapDS(0)
    for (cnt <- 0 to mapDS.size-1) {
      if(cnt > 1) {
        ds = ds.union(mapDS(cnt))
      }
    }
    ds.repartition(1).write.mode("overwrite").option("header","true")
      .csv(s"${pathWriteDiscovery}/results_cat${category}")

  }

  def main(args: Array[String]): Unit = {
    run()

  }
}
