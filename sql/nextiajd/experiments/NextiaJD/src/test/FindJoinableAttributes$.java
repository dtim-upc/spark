object FindJoinableAttributes {

  lazy val spark = SparkSession.builder.appName("SparkSQL")
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .config("spark.driver.memory", "5g")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()

  import spark.implicits._

  val pathSet = "/Users/javierflores/Documents/Programming/FindDS/resources/sets"
  val path ="/Users/javierflores/Documents/Programming/FindDS/"


  def normalizeStr(input: String): String = {
    if(input == null || input == ""){
      input
    }else{
      Normalizer.normalize(input, java.text.Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
    }
  }
  val normalizeStrUDF = udf(normalizeStr(_: String): String)


  def toSets(ds: DataFrame, filename:String ,att: String) = {
    val name = s"${filename}.${att}"
    ds.write.parquet(s"${pathSet}/${name}")
  }

  def readDS(filename: String, delim : String, multiline: String):DataFrame = {

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delim)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiline",multiline)
      .csv(s"$path/$filename")
  }

  def findCandidates(f1: String, ds1:DataFrame, f2: String, ds2: DataFrame, typeAtt: String): Seq[(String, String, String, String, String, String, String, String)]= typeAtt match{
    case "nominal" =>

      val names1 = ds1.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
      val names2 = ds2.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
      find(f1,ds1,f2,ds2,names1,names2)

  }

  def find(f1: String, ds1:DataFrame, f2: String, ds2: DataFrame, names1:Seq[String],names2:Seq[String]): Seq[(String, String, String, String, String, String, String, String)]={
    //  var comparisons: Seq[String] = Nil
    var lines: Seq[(String, String, String, String,String, String, String, String )] = Nil
    if(names1.size >0 && names2.size>0){
      println("finding nominals...")
    }
    for( att1 <- names1 ){
      for( att2 <- names2 ){

        var d1 = ds1
        var d2 = ds2

        try {
          d1 = spark.read.parquet(s"${pathSet}/${f1}.${att1}")
        } catch {
          case e:
            AnalysisException => println("Couldn't find that file.")
            //d1 = ds1.select(att1).distinct()
            d1 = ds1.select(lower(trim(normalizeStrUDF(col(att1)))).as(att1)).na.drop.distinct()
            toSets(d1, f1, att1)
        }
        try {
          d2 = spark.read.parquet(s"${pathSet}/${f2}.${att2}")
        } catch {
          case e:
            AnalysisException => println("Couldn't find that file.")
            d2 = ds2.select(lower(trim(normalizeStrUDF(col(att2)))).as(att2)).na.drop.distinct()
            //d2 = ds2.select(att2).distinct()
            toSets(d2, f2, att2)
        }




        val size1 = d1.count().toDouble
        val size2 = d2.count().toDouble

        val joinExpression = d1.col(att1) === d2.col(att2)
        val j = d1.join(d2,joinExpression)
        val tuplas = j.count().toDouble
        val containment1 = tuplas.toDouble/size1
        val containment2 = tuplas.toDouble/size2
        val jaccard = 0



        // file1, att1, size1, file2, att2, size2, tuplas, containment1
        lines =lines:+(f1,att1,s"$size1",f2,att2,s"$size2", s"$tuplas", s"$containment1")
        lines =lines:+(f2,att2,s"$size2",s"$f1",att1,s"$size1", s"$tuplas", s"$containment2")

        // val tmp = s"${f1},${att1},${size1},${f2},${att2},${size2},${tuplas},${containment1},${containment2},${jaccard}\n"
        //println(tmp)
        //comparisons = comparisons :+ tmp
      }
    }
    lines
  }



  def findMain(): Unit ={
    var lines = Seq[(String, String, String, String,String, String, String, String )]()

    val dsInfo = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").
      load(s"${path}/resources/datasetInformationExperiment2.csv")

    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename","delimiter", "multiline").collect()
      .foreach{case Row( filename: String, delimiter: String, multiline: String) => mapDS = mapDS + (filename -> readDS(filename, delimiter, multiline))}

    val listFiles = mapDS.keySet.toSeq
    val size = listFiles.size
    println(size)

    // new time

    val t1 = System.nanoTime
    var lines2: Seq[(String, String, String, String,String, String, String, String )] = Nil


    for(i <- 0 to size-2){
      for(j <- i+1 to size-1){
        println(s"${listFiles(i)} - ${listFiles(j)}\n")
        val ds1 = mapDS.get(listFiles(i)).get
        val ds2 =mapDS.get(listFiles(j)).get
        lines2 = lines2 ++ findCandidates(listFiles(i),ds1,listFiles(j),ds2,"nominal")


      }
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"execution time:  ${duration} seconds")

    val someDF = lines2.toDF("file1", "att1","sizeDistinct1","file2","att2","sizeDistinct2","joinSize","containment")

  }

  def main(args: Array[String]): Unit = {

    val pathDatasets = ""
    val pathSets = s"$pathDatasets/sets"
    val from = 1
    val to = 10
    val datasetsInfo = ""

    val s = args(0)
  }

}
