/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.File

import scala.reflect.io.Directory
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, StddevPop}
import org.apache.spark.sql.execution.stat.StatMetaFeature.getMetaFeatures
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf.emptyMF
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array_intersect, col, greatest, least, length, levenshtein, lit, lower, size, trim, udf, when}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.utils.Unzip



object FindQualityJoins {

  def findQualityJoins(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
                       queryAtt: String = "", filterEnable: String = "true"): DataFrame = {
    val distances = preDist(queryDataset, candidatesDatasets, queryAtt, filterEnable)

    val filename = queryDataset.inputFiles(0).split("/").last
    val pathDiscoveryTmp = queryDataset.inputFiles(0).replace(filename, "discoveryTmp")

    distances.write.mode(SaveMode.Overwrite).format("parquet").save(pathDiscoveryTmp)

    val distancescomputed = queryDataset.sparkSession.read.load(pathDiscoveryTmp).na.fill(0)
//    predict(distancescomputed)

    val discovery = predict(distancescomputed)
    val directory = new Directory(new File(pathDiscoveryTmp))
    directory.deleteRecursively()
    discovery
  }



  def preDist(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
                       queryAtt: String = "", filterEnable: String = "true"): DataFrame = {
    val profiles = getProfiles(queryDataset, candidatesDatasets, queryAtt, filterEnable)
    val normalizedP = normalizeProfiles(profiles, "nominal")
    val fileName = queryDataset.inputFiles(0).split("/").last
    val pairs = createPairs(fileName, normalizedP)
    distances(pairs)
  }

  def getProfiles(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
                  queryAtt: String = "", filterEnable: String = "true"): DataFrame = {

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

   if(filterEnable == "true") {

     profiles = profiles.filter(col(emptyMF.name) === 0)
      .filter(col("dataType") =!= "numeric")
      .filter(col("specificType") =!= "phone")
      .filter(col("specificType") =!= "datetime")
      .filter(col("specificType") =!= "time")
      .filter(col("specificType") =!= "date")
   } else {
     profiles = profiles.filter(col(emptyMF.name) === 0)
   }


    profiles = profiles.withColumn("bestContainment", col("bestContainment").cast(DoubleType))
    profiles
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
  }

  def createPairs(fileName: String, normalizeProfiles: DataFrame): DataFrame = {

    val metafeatures = normalizeProfiles.schema.map(_.name)

    val candidateAtt = normalizeProfiles.filter(col("ds_name") =!= fileName)
      .select(metafeatures.map(x => col(x).as(s"${x}_2")): _*)

    val queryAtt = normalizeProfiles.filter(col("ds_name") === fileName)

    if (queryAtt.count() == 1) {

      queryAtt.withColumn("key", lit("1"))
              .join(candidateAtt.withColumn("key", lit("1")), "key")
                .drop("key")


    } else {

      queryAtt.crossJoin(candidateAtt)
    }
  }


  def distances(pairsAtt: DataFrame): DataFrame = {
    var pairs = pairsAtt

    pairs = pairs.withColumn("flippedContainment",
      least(col("bestContainment"), col("bestContainment_2"))
        /greatest(col("bestContainment"), col("bestContainment_2"))
    )

    pairs = pairs.withColumn("worstBestContainment", col("flippedContainment"))

    val metaFeatures = getMetaFeatures("nominal")

    for (metafeature <- metaFeatures.filter(_.normalize)) {
      metafeature.normalizeType match {
        case 3 => // probably delete it
          pairs = pairs.withColumn(metafeature.name,
            size(array_intersect(col(metafeature.name), col(s"${metafeature.name}_2")))
              /greatest(size(col(metafeature.name)), size(col(s"${metafeature.name}_2")))
          )
        case 2 =>
          pairs = pairs.withColumn(metafeature.name,
            size(array_intersect(col(metafeature.name), col(s"${metafeature.name}_2")))
              /greatest(size(col(metafeature.name)), size(col(s"${metafeature.name}_2")))
          )
        case 1 => // edit distance
          pairs = pairs.withColumn(metafeature.name,
            levenshtein(
              lower(trim(col(metafeature.name))),
              lower(trim(col(s"${metafeature.name}_2")))
            )/ greatest(length(col(metafeature.name)), length(col(s"${metafeature.name}_2")))
          )

        case 5 =>

          pairs = pairs.withColumn("bestContainment",
            least(col("bestContainment"), col("bestContainment_2"))/col("bestContainment")
          )

        case _ => // 0 y 4
          pairs = pairs.withColumn(
            metafeature.name, col(metafeature.name) - col(s"${metafeature.name}_2"))

      }
    }

    pairs = pairs.withColumn(
      "name_dist", levenshtein(lower(trim(col("att_name"))), lower(trim(col("att_name_2"))))
        / greatest(length(col("att_name")), length(col("att_name_2")))
    )
    pairs = pairs.drop(metaFeatures.filter(_.normalize).map(x => s"${x.name}_2"): _*)
    pairs
  }

  def predictChain(matchingNom: Dataset[_], getPredRuled: UserDefinedFunction): DataFrame = {

    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/modelsChain.zip") )

    val cvModel0 = CrossValidatorModel.load(s"${pathM}/models/class0")
    val cvModel1 = CrossValidatorModel.load(s"${pathM}/models/class1")
    val cvModel2 = CrossValidatorModel.load(s"${pathM}/models/class2")
    val cvModel3 = CrossValidatorModel.load(s"${pathM}/models/class3")
    val cvModel4 = CrossValidatorModel.load(s"${pathM}/models/class4")

    val dropCols = Seq("features", "rawPrediction", "probability", "prediction")

    val p0 = cvModel0.transform(matchingNom)

    val resultsM0 = p0.withColumn("p0", second(col("probability")))
      .drop(dropCols: _*)

    val p1 = cvModel1.transform(resultsM0)

    val resultsM1 = p1.withColumn("p1", second(col("probability"))).drop(dropCols: _*)

    val p2 = cvModel2.transform(resultsM1)

    val resultsM2 = p2.withColumn("p2", second(col("probability"))).drop(dropCols: _*)


    val p3 = cvModel3.transform(resultsM2)

    val resultsM3 = p3.withColumn("p3", second(col("probability"))).drop(dropCols: _*)

    val predAll = cvModel4.transform(resultsM3).withColumn("p4", second(col("probability")))
      .drop(dropCols: _*).select("ds_name", "att_name", "ds_name_2", "att_name_2", "p0", "p1",
      "p2", "p3", "p4", "flippedContainment")


    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()

    predAll.withColumn("prediction", getPredRuled(
        col("p0"), col("p1"), col("p2"), col("p3"), col("p4"),
        col("flippedContainment")
      ) ).select("ds_name", "att_name", "ds_name_2", "att_name_2", "prediction",
      "p0", "p1", "p2", "p3", "p4")


  }

  def predict(matchingNom: Dataset[_]): DataFrame = {
    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/modelsThesis.zip") )

    val modelClass0 = CrossValidatorModel.load(s"${pathM}/models/class0")
    val modelClass1 = CrossValidatorModel.load(s"${pathM}/models/class1")
    val modelClass2 = CrossValidatorModel.load(s"${pathM}/models/class2")
    val modelClass3 = CrossValidatorModel.load(s"${pathM}/models/class3")
    val modelClass4 = CrossValidatorModel.load(s"${pathM}/models/class4")

    val pred0 = modelClass0.transform(matchingNom)
      .withColumnRenamed("probability", "probability0")
      .withColumn("probability0", second(col("probability0")))
    val pred1 = modelClass1.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability1")
      .withColumn("probability1", second(col("probability1")))

    val pred2 = modelClass2.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability2")
      .withColumn("probability2", second(col("probability2")))

    val pred3 = modelClass3.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability3")
      .withColumn("probability3", second(col("probability3")))

    val pred4 = modelClass4.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability4")
      .withColumn("probability4", second(col("probability4")))

    val basicCols = Seq("ds_name", "att_name", "ds_name_2", "att_name_2")
    val predAll = pred0
      .join(pred1, basicCols)
      .join(pred2, basicCols)
      .join(pred3, basicCols)
      .join(pred4, basicCols)


    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()



    predAll.withColumn("prediction", getPredRuled3(
      col("probability0"),
      col("probability1"),
      col("probability2"),
      col("probability3"),
      col("probability4"),
      col("flippedContainment")
    ) ).select("ds_name", "att_name", "ds_name_2", "att_name_2", "prediction",
      "probability0", "probability1", "probability2", "probability3", "probability4")
  }

  def getPredictionRuled3(num0: Double, num1: Double, num2: Double, num3: Double,
                          num4: Double, worstCnt: Double ): Int = {
    var s = Seq(num0, num1, num2, num3, num4).zipWithIndex
    var index: Int = 0

    var i = 0
    while (i <= 4) {
      index = s.maxBy(_._1)._2


      if (validateLabel(index, worstCnt) ) {
        i = 5
      } else {
        s = s.filter(_._2 != index)
      }
      i = i + 1
    }

    if (index != 0) {
      val diff0 = (s(index)._1 -num0).abs
      if (diff0 <= 0.1  || num0 >= 0.5 ) {
        index = s.filter(_._2 < index).maxBy(_._1)._2
      }
    }
    index


  }

  def validateLabel(x: Double, flippedCnt: Double): Boolean = x match {
    case 2 =>
      if (flippedCnt >= 0.082) true else false
    case 3 =>
      if (flippedCnt >= 0.125) true else false
    case 4 =>
      if (flippedCnt >= 0.25) true else false
    case _ =>
      true
  }


  val getPredRuled3 = udf(getPredictionRuled3(_: Double, _: Double, _: Double
    , _: Double, _: Double, _: Double): Int)

  val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))



  def findQualityJoinsTmp(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
                          getPredRuled: UserDefinedFunction,
                          filterEnable: String = "true"): DataFrame = {

    val distances = preDist(queryDataset, candidatesDatasets, "", filterEnable)

    val filename = queryDataset.inputFiles(0).split("/").last
    val pathDiscoveryTmp = queryDataset.inputFiles(0).replace(filename, "discoveryTmp")

    distances.write.mode(SaveMode.Overwrite).format("parquet").save(pathDiscoveryTmp)

    val distancescomputed = queryDataset.sparkSession.read.load(pathDiscoveryTmp).na.fill(0)

    val discovery = predictChain(distancescomputed, getPredRuled)
    val directory = new Directory(new File(pathDiscoveryTmp))
    directory.deleteRecursively()
    discovery
  }

  def predictTmp(matchingNom: Dataset[_], getPredRuled: UserDefinedFunction): DataFrame = {
    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/modelsThesis.zip") )

    val modelClass0 = CrossValidatorModel.load(s"${pathM}/models/class0")
    val modelClass1 = CrossValidatorModel.load(s"${pathM}/models/class1")
    val modelClass2 = CrossValidatorModel.load(s"${pathM}/models/class2")
    val modelClass3 = CrossValidatorModel.load(s"${pathM}/models/class3")
    val modelClass4 = CrossValidatorModel.load(s"${pathM}/models/class4")

    val pred0 = modelClass0.transform(matchingNom)
      .withColumnRenamed("probability", "probability0")
      .withColumn("probability0", second(col("probability0")))
    val pred1 = modelClass1.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability1")
      .withColumn("probability1", second(col("probability1")))

    val pred2 = modelClass2.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability2")
      .withColumn("probability2", second(col("probability2")))

    val pred3 = modelClass3.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability3")
      .withColumn("probability3", second(col("probability3")))

    val pred4 = modelClass4.transform(matchingNom)
      .select("ds_name", "att_name", "ds_name_2", "att_name_2", "probability")
      .withColumnRenamed("probability", "probability4")
      .withColumn("probability4", second(col("probability4")))

    val basicCols = Seq("ds_name", "att_name", "ds_name_2", "att_name_2")
    val predAll = pred0
      .join(pred1, basicCols)
      .join(pred2, basicCols)
      .join(pred3, basicCols)
      .join(pred4, basicCols)


    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()



    predAll.withColumn("prediction", getPredRuled(
      col("probability0"),
      col("probability1"),
      col("probability2"),
      col("probability3"),
      col("probability4"),
      col("flippedContainment")
    ) ).select("ds_name", "att_name", "ds_name_2", "att_name_2", "prediction",
      "probability0", "probability1", "probability2", "probability3", "probability4")
  }


}

