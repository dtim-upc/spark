import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, StddevPop}
import org.apache.spark.sql.execution.stat.StatMetaFeature.{getMetaFeatures, normalizeDF}
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf.emptyMF
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType

//object fqj {
//
//
//
//
//}

object findQualityJJ {

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

    normalizeProfiles(profiles, "nominal")

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

}
