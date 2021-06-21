import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ParallelGroundTruth {

  val spark = SparkSession.builder.appName("SparkSQL")
    .master("local[*]")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.maxResultSize", "4g")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  def arrayItem(name: String) = struct(lit(name) as "colName",$"$name" as "colVal")

  def readDataset(path: String): Dataset[Row] = {
    val dataset = spark.read.parquet(path)
    val datasetN = dataset.withColumn("tmp", explode(array(dataset.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name).map(arrayItem): _*)))
      .withColumn("dataset", input_file_name())
      .select($"dataset", $"tmp.colName", $"tmp.colVal")
      .withColumn("colName", trim(lower($"colName")))
      .withColumn("colVal", trim(lower($"colVal")))
      .filter($"colVal" =!= "") //filter empty strings
      .groupBy($"dataset",$"colName")
      .agg(collect_set($"colVal").alias("values")) //collect_set eliminates duplicates
      .withColumn("cardinality", size($"values"))
    datasetN
  }

  def main(args: Array[String]): Unit = {
    val all = os.list(os.Path("/home/snadal/UPC/Projects/NextiaJD/ParallelComputeGroundTruth/data/"))
      .map(p => readDataset(p.toString()))
      .reduce(_.unionAll(_))

    val A = all.withColumnRenamed("dataset","datasetA")
      .withColumnRenamed("colName", "attA")
      .withColumnRenamed("values","valuesA")
      .withColumnRenamed("cardinality","cardinalityA")

    val B = all.withColumnRenamed("dataset","datasetB")
      .withColumnRenamed("colName", "attB")
      .withColumnRenamed("values","valuesB")
      .withColumnRenamed("cardinality","cardinalityB")

    val cross = A.crossJoin(B)
      .filter($"datasetA" =!= $"datasetB") //avoid self-joins
      .withColumn("C", size(array_intersect($"valuesA",$"valuesB"))/$"cardinalityA")
      .withColumn("K", least($"cardinalityA",$"cardinalityB")/greatest($"cardinalityA",$"cardinalityB"))
      .select($"datasetA",$"attA",$"datasetB",$"attB",$"C",$"K")

    cross.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv("out.csv")
  }
}