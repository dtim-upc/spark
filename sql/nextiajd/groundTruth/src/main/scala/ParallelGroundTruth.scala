import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ParallelGroundTruth {

  def readDataset(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._

    val dataset = spark.read.parquet(path)
    val datasetN = dataset.withColumn("tmp",
      explode(array(dataset.schema.filter(a => a.dataType.isInstanceOf[StringType])
        .map(a => a.name).map(name => struct(lit(name) as "colName",$"$name" as "colVal")): _*)))
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
    if (args.length != 2) {
      throw new Exception("Two arguments required (directory with parquet files, and output to write CSV)")
    }

    val spark = SparkSession.builder.appName("ComputeGroundTruth")./*master("local[*]").*/getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val all = os.list(os.Path(args(0)))
      .map(p => readDataset(spark,p.toString()))
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
      .filter($"C" > 0)
      .withColumn("K", least($"cardinalityA",$"cardinalityB")/greatest($"cardinalityA",$"cardinalityB"))
      .select($"datasetA",$"attA",$"datasetB",$"attB",$"C",$"K")

    cross.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(args(1))
  }
}