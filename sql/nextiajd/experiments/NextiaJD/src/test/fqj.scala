import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, StddevPop}
import org.apache.spark.sql.execution.stat.StatMetaFeature.{getMetaFeatures, normalizeDF}
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf.emptyMF
import org.apache.spark.sql.functions.{array_intersect, col, least, lit, greatest, when, levenshtein, lower, trim, length, size}
import org.apache.spark.sql.types.{DoubleType, StringType}

//object fqj {
//
//
//
//
//}

object findQualityJ {

  var isQueryByAtt = false

  def getProfiles(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame], queryAtt:String = ""): DataFrame ={

    var profiles = queryDataset.metaFeatures
    if (queryAtt != "") {
      profiles = queryDataset.metaFeatures.filter(col("att_name") === queryAtt)
    }

    for (i <- 0 to candidatesDatasets.size-1) {
      var profilesTmp = candidatesDatasets(i).metaFeatures
      if (!profilesTmp.head(1).isEmpty) {
        profiles = profiles.union(profilesTmp)
      }
    }

    profiles = profiles.filter(col(emptyMF.name) === 0)
//      .filter(col("dataType") =!= NumericStr || col("dataType") =!= DateTimeStr)

//    print(profiles.count())
//    profiles.select("bestContainment").show(117,false)
    profiles = profiles.withColumn("bestContainment", col("bestContainment").cast(DoubleType))
    val normalizedP = normalizeProfiles(profiles, "nominal")
    val fileName = queryDataset.inputFiles(0).split("/").last
    createPairs(fileName, normalizedP)
  }

  def normalizeProfiles(df: Dataset[_], metaType: String): DataFrame = {

    val cols = getMetaFeatures("nominal").filter(_.normalize).filter(_.normalizeType == 0)

    val aggExprsAvg = Seq((child: Expression) => Average(child).toAggregateExpression())
      .flatMap { func => cols.map(c => new Column(Cast(func(new Column(c.name).expr), StringType))
        .as(s"${c.name}_avg"))
      }
    val aggExprsSD = Seq((child: Expression) => StddevPop(child).toAggregateExpression())
      .flatMap { func => cols.map(c => new Column(Cast(func(new Column(c.name).expr), StringType))
        .as(s"${c.name}_sd"))
      }
    val aggExprs = aggExprsAvg ++ aggExprsSD
    val avgSD = df.select(aggExprs: _*).take(1).head

    var zScoreDF = df
    val colnames = cols.map(_.name)
    for (c <- colnames) {
      zScoreDF = zScoreDF.withColumn(c, when(lit(avgSD.getAs(s"${c}_sd")) === 0,
        (col(c) - lit(avgSD.getAs(s"${c}_avg"))) /  lit(1))
        .otherwise((col(c) - lit(avgSD.getAs(s"${c}_avg"))) / lit(avgSD.getAs(s"${c}_sd")))
      )
    }
    zScoreDF.toDF()
//    df.sparkSession.createDataFrame(dataF.rdd, dataF.schema).cache()

  }

  def createPairs(fileName: String, normalizeProfiles: DataFrame): DataFrame = {

    val metafeatures = normalizeProfiles.schema.map(_.name)

    val candidateAtt = normalizeProfiles.filter(col("ds_name") =!= fileName)
      .select(metafeatures.map(x => col(x).as(s"${x}_2")): _*)

    val queryAtt = normalizeProfiles.filter(col("ds_name") === fileName)
    var pairs = queryAtt.crossJoin(candidateAtt)

    distances(pairs)
//

  }


  def distances(pairsAtt:DataFrame): DataFrame = {
    var pairs = pairsAtt

    pairs = pairs.withColumn("flippedContainment",
      least(col("bestContainment"),col("bestContainment_2"))/greatest(col("bestContainment"),col("bestContainment_2"))
    )

    pairs = pairs.withColumn("worstBestContainment", col("flippedContainment"))

    val metaFeatures = getMetaFeatures("nominal")

    for (metafeature <- metaFeatures.filter(_.normalize)) {
      metafeature.normalizeType match {
        case 3 => //probably delete it
          pairs = pairs.withColumn(metafeature.name,
            size(array_intersect(col(metafeature.name), col(s"${metafeature.name}_2")))/greatest(size(col(metafeature.name)), size(col(s"${metafeature.name}_2")))
          )
        case 2 =>
          pairs = pairs.withColumn(metafeature.name,
            size(array_intersect(col(metafeature.name), col(s"${metafeature.name}_2")))/greatest(size(col(metafeature.name)), size(col(s"${metafeature.name}_2")))
          )
        case 1 => // edit distance
          pairs = pairs.withColumn(metafeature.name,
            levenshtein(
              lower(trim(col(metafeature.name))),
              lower(trim(col(s"${metafeature.name}_2")))
            )/ greatest(length(col(metafeature.name)),length(col(s"${metafeature.name}_2")))
          )

        case 5 =>

          pairs = pairs.withColumn(metafeature.name,
            least(col(metafeature.name),col(s"${metafeature.name}_2"))/col(metafeature.name)
          )

        case _ => // 0 y 4
          pairs = pairs.withColumn(
            metafeature.name, col(metafeature.name) - col(s"${metafeature.name}_2"))

      }
    }

    pairs = pairs.withColumn(
      "name_dist", levenshtein(lower(trim(col("att_name"))), lower(trim(col("att_name_2"))))
        / greatest(length(col("att_name")),length(col("att_name_2")))
    )
    pairs = pairs.drop(metaFeatures.filter(_.normalize).map(x => s"${x.name}_2"): _*)
    pairs
  }

}
