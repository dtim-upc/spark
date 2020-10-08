object Distances {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.sql.autoBroadcastJoinThreshold", "7200")
      .config("spark.sql.broadcastTimeout", "7200")
      .getOrCreate()

    val path ="/Users/javierflores/Documents/Research/Projects/FJA/Datasets"
    val folderDs = "raw"

    def readDS(filename: String, delim : String, multiline: Boolean):DataFrame = {
      spark.read
        .option("header", "true").option("inferSchema", "true")
        .option("delimiter", delim).option("quote", "\"")
        .option("escape", "\"").option("multiline",multiline)
        .csv(s"$path/$folderDs/$filename")
    }

    val dsInfo = spark.read
      .option("header", "true").option("inferSchema", "true").
      csv(s"${path}/datasetInformation.csv")

    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename","delimiter", "multiline").collect()
      .foreach{
        case Row( filename: String, delimiter: String, multiline:Boolean) =>
          mapDS = mapDS + (filename -> readDS(filename,delimiter,multiline))
      }


    val listFiles = mapDS.keySet.toSeq
    val size = listFiles.size
    println(s"datasets: $size")


    val pairsDatasets = spark.read
      .option("header", "true").option("inferSchema", "true").
      csv(s"${path}/rawAttributesGroundTruth.csv")
      .filter(col("containment")>0)

    var queryDatasets:Seq[String] = Nil


    pairsDatasets.select("file1").distinct().collect()
      .foreach{
        case Row( filename: String) =>
          queryDatasets = queryDatasets :+ filename
      }


    var cnt = 1

    val t1 = System.nanoTime
    for(f <- queryDatasets){

      val queryDataset = mapDS.get(f).get

      println(f)

      var candidateDatasets:Seq[DataFrame] = Nil
      val cand = pairsDatasets.filter(col("file1") === f).select("file2").distinct().collect()

      for (row <- cand){
        candidateDatasets = candidateDatasets :+ mapDS.get(row(0).toString).get
      }

      println(candidateDatasets.length)

      var a = FindQualityJoins.preDist(queryDataset,candidateDatasets)

      a.repartition(1).write.mode("overwrite").option("header","true")
        .csv(s"${path}/distancesFinalTest/dataDistances${cnt}.csv")
      cnt = cnt+1

      val duration = (System.nanoTime - t1) / 1e9d
      println(s"time:  ${duration} seconds")



    }

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"execution time:  ${duration} seconds")



    val a = 1
    var df = spark.read.option("header", "true").option("inferSchema", "true").
      csv(s"${path}/distancesFinalTest/dataDistances${a}.csv")

    for(cnt <- 2 to queryDatasets.size-3){

      val dfT = spark.read.option("header", "true").option("inferSchema", "true").
        csv(s"${path}/distancesFinalTest/dataDistances${cnt}.csv")
      if (dfT.schema.size != 0) {
        df = df.union(dfT)
      }else{
        println("empty")
      }

    }

    df.repartition(1).write.mode("overwrite").option("header","true")
      //.csv(s"${path}/resources/distancesAll/distancesNominalv6")
      //resources/tmp/distancesIntegratedTest/nomv5/distances5
      .csv(s"${path}/distancesAll")


    spark.close()
  }
}
