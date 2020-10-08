object Comparison {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.maxResultSize", "4g")
    .getOrCreate()



  import spark.implicits._

  val path = "/Users/javierflores/Documents/Research/Projects/FJA/Comparison/src/resources"

  def readDataset(file: String, sep: String):DataFrame = {
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", sep)
    .option("quote","\"")
    .option("escape", "\"")
    .csv(file)
  }

  def run(): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

//    println(s"$path/datasetInformationExperiment2.csv")
    val dsInfo = readDataset(s"$path/datasetInformationExperiment2.csv",",")

//    dsInfo.show()
    var mapDS = Map[String, DataFrame]()

    dsInfo.select("filename", "separator").collect()
      .foreach{
        case Row( filename: String, separator: String) =>
          mapDS = mapDS + (filename -> readDataset(s"$path/datasets/$filename", separator))
      }
//    mapDS.keySet.map(println)




    // set query dataset
    val queryAtt = "atomid"
    val queryDs = "carcinogenesis_sbond_3.csv"

    val queryDataset = mapDS(queryDs)
    val candidateDatasets = mapDS.-(queryDs).map(_._2).toSeq

//    findQualityJ.freqContainment(queryDataset.metaFeatures)
//    freqWordContainment
    val t1 = System.nanoTime
    val a = FindQualityJoins.preDist(queryDataset,candidateDatasets)
    //113
    a.show(339)
//    findQualityJ.getProfiles(queryDataset,candidateDatasets)
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"execution time:  ${duration} seconds")
  }

  def main(args: Array[String]): Unit = {
    run()

  }
}
