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
import java.text.Normalizer

import scala.collection.mutable.Map
import scala.collection.parallel.ParSeq
import scala.math.max
import scala.reflect.io.Directory

import org.apache.spark.internal.Logging
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.stat.StatMetaFeature.{getMetaFeatures, normalizeDF}
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeature
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf._
import org.apache.spark.sql.functions.{abs, col, desc, input_file_name, lit, lower, trim, udf}
import org.apache.spark.sql.types.{DoubleType, NumericType, StringType}
import org.apache.spark.sql.utils.Unzip

// renamed from findJoins to FQJ
object FQJ extends  Logging {


  var currentQueryDataset = ""
  var candidatesNumber = 0
  var inCache = false
  var isQueryByAttr = false

//  var setK0: Dataset[_]
//  var setK1: Dataset[_]
//  var setK2: Dataset[_]
//  var setK3: Dataset[_]
//  var setK4: Dataset[_]


  def findQualityJoinsIncrementally(dfSource: DataFrame, dfCandidates: Seq[DataFrame],
                                    queryAtt: String = "", k: Int): DataFrame = {


    if (queryAtt == "") {
      // discovery by query-by-dataset
      val tmp = computeDistancesByDataset(dfSource, dfCandidates) // nomZ, pairs
      val matchingNom = tmp._2
      predict(matchingNom)
    } else {
      val tmp = computeDistancesByDataset(dfSource, dfCandidates, queryAtt)
      val matchingNom = tmp._2
      predict(matchingNom)
    }

  }

//
//  def createPairsInc(dfSource: DataFrame, dfCandidates: Seq[DataFrame],
//                     queryAtt: String = ""): (Dataset[_], Dataset[_]) = {
//    val fileName = dfSource.inputFiles(0).split("/").last
//    var dfQ = dfSource
//    var nominalMetaFeatures = dfQ.metaFeatures
//    if (queryAtt != "") {
//      dfQ = dfSource.select(queryAtt)
//      nominalMetaFeatures = nominalMetaFeatures.filter(col("att_name") === queryAtt)
//    }
//
//
//    val columns = dfSource.schema.map(_.name)
//
//    val nomAttCandidates = dfSource.filter(col("ds_name") =!= fileName)
//      .select(columns.map(x => col(x).as(s"${x}_2")): _*).cache()
//
//    // select the normalized meta-features from the dataset source
//    // perform a cross join with the attributes from other datasets
//    val tmp = zScoreDF.filter(col("ds_name") === fileName).crossJoin(nomAttCandidates)
//    // forcing cache of the distances
//    var attributesPairs = zScoreDF.sparkSession.createDataFrame(tmp.rdd, tmp.schema).cache()
//
//
//  }

  def pathMF(path: String, fn1: String, meta: String): String = {
    val fn = fn1.replace(".", "")
    path.replace(fn1, s".${fn}/.${meta}")
  }


  var queryByAttDiscoveryPath = ""
  var queryByDsDiscoveryPath = ""


  def findQualityJoins(dfSource: DataFrame, dfCandidates: Seq[DataFrame], queryAtt: String = ""):
  DataFrame = {

//    val filename = dfSource.inputFiles(0).split("/").last
//    if(filename != currentQueryDataset && candidatesNumber != dfCandidates.size) {
//      currentQueryDataset = filename
//      inCache = false
//      if(queryAtt != "") {
//        isQueryByAttr = true
//      } else {
//        isQueryByAttr = false
//      }
//    } else {
//      inCache = true
//    }
    currentQueryDataset = dfSource.inputFiles(0).split("/").last
    if(demo) {
      val filename = currentQueryDataset
      val path = dfSource.inputFiles(0)

      // scalastyle:off println
//      println(" DEMO")
      // scalastyle:on println
      val nomPath = pathMF(s"${path}QUERY", filename, s"${dfCandidates.size}.${queryAtt}")
      // scalastyle:off println
//      println(s"$nomPath")
      // scalastyle:on println
      try {
        val matchingNom = dfSource.sparkSession.read.load(nomPath)
        // scalastyle:off println
//        println(s"read")
        // scalastyle:on println
        predict(matchingNom)
        val predPath = pathMF(s"${path}Prediction", filename, s"${dfCandidates.size}.${queryAtt}")
        predict(matchingNom).write.mode(SaveMode.Overwrite).format("parquet").save(predPath)
        if(queryAtt == "") {
          queryByDsDiscoveryPath = predPath
          dfSource.sparkSession.read.load(predPath).orderBy(desc("prediction"))
        } else {
          queryByAttDiscoveryPath = predPath
          dfSource.sparkSession.read.load(predPath).orderBy(desc("prediction"))
        }
      } catch {
        case e:
          AnalysisException =>
          // scalastyle:off println
//          println("catch")
          // scalastyle:on println
          val tmp = computeDistancesByDataset(dfSource, dfCandidates, queryAtt) // nomZ, pairs
          val matchingNom = tmp._2
          matchingNom.write.mode(SaveMode.Overwrite).format("parquet").save(nomPath)

          val predPath = pathMF(s"${path}Prediction", filename, s"${dfCandidates.size}.${queryAtt}")

          predict(matchingNom).write.mode(SaveMode.Overwrite).format("parquet").save(predPath)
          if(queryAtt == "") {
            queryByDsDiscoveryPath = predPath
              dfSource.sparkSession.read.load(predPath).orderBy(desc("prediction"))
          } else {
            queryByAttDiscoveryPath = predPath
            dfSource.sparkSession.read.load(predPath).orderBy(desc("prediction"))
          }
      }
    } else {
      aux(dfSource, dfCandidates, queryAtt)
    }

  }

  def getLastAttDiscovery(df: DataFrame): DataFrame = {
    df.sparkSession.read.load(queryByAttDiscoveryPath).orderBy(desc("prediction"))
  }
  def getLastDatasetDiscovery(df: DataFrame): DataFrame = {
    df.sparkSession.read.load(queryByDsDiscoveryPath).orderBy(desc("prediction"))
  }

  def aux(dfSource: DataFrame, dfCandidates: Seq[DataFrame], queryAtt: String): DataFrame = {
    if (queryAtt == "") {
      // discovery by query-by-dataset
      val tmp = computeDistancesByDataset(dfSource, dfCandidates) // nomZ, pairs
      val matchingNom = tmp._2
      predict(matchingNom).orderBy(desc("prediction"))
    } else {
      val tmp = computeDistancesByDataset(dfSource, dfCandidates, queryAtt)
      val matchingNom = tmp._2
      predict(matchingNom).orderBy(desc("prediction"))
    }
  }


  def predict(matchingNom: Dataset[_]): DataFrame = {
    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/models.zip") )

    val modelClass0 = CrossValidatorModel.load(s"${pathM}/class0")
    val modelClass1 = CrossValidatorModel.load(s"${pathM}/class1")
    val modelClass2 = CrossValidatorModel.load(s"${pathM}/class2")
    val modelClass3 = CrossValidatorModel.load(s"${pathM}/class3")
    val modelClass4 = CrossValidatorModel.load(s"${pathM}/class4")

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
      ) ).select("ds_name", "att_name", "ds_name_2", "att_name_2", "prediction")
  }

  def getPredictionRuled3(num0: Double, num1: Double, num2: Double, num3: Double,
                          num4: Double, worstCnt: Double ): Int = {
    var s = Seq(num0, num1, num2, num3, num4).zipWithIndex
    var index = 0


    var found = false
    while (!found) {

      index = s.maxBy(_._1)._2
      if (index == 4 && worstCnt < 0.2 ) {
        s = s.filter(_._2 != index)
      } else if (index == 3 && worstCnt < 0.12 ) {
        s = s.filter(_._2 != index)
      } else if (index == 2 && worstCnt < 0.08 ) {
        s = s.filter(_._2 != index)
      } else if (index == 1 && worstCnt < 0.05 ) {
        s = s.filter(_._2 != index)
      } else {
        found = true
      }

    }
    if (index != 0) {

      val diff1 = (s(index)._1 -num1).abs
      val diff0 = (s(index)._1 -num0).abs
      if (diff0 <= 0.1  || num0 >= 0.5 ) {
        index = index -1

      } else if (index == 2 && diff1 <= 0.1) {
        index = index-1
      }
    }
    index
  }


  val getPredRuled3 = udf(getPredictionRuled3(_: Double, _: Double, _: Double
    , _: Double, _: Double, _: Double): Int)

  val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))

  def computeDistancesByDataset(dfSource: DataFrame, dfCandidates: Seq[DataFrame],
                                queryAtt: String = ""): (Dataset[_], Dataset[_]) = {


    var dfQ = dfSource
    var nominalMetaFeatures = dfQ.metaFeatures

    if (queryAtt != "") {
      dfQ = dfSource.select(queryAtt)
      nominalMetaFeatures = dfQ.metaFeatures.filter(col("att_name") === queryAtt)
    }


    for (i <- 0 to dfCandidates.size-1) {
      var nominalMetaFeaturesTmp = dfCandidates(i).metaFeatures
      if (!nominalMetaFeaturesTmp.head(1).isEmpty) {
        nominalMetaFeatures = nominalMetaFeatures.union(nominalMetaFeaturesTmp)
      }
    }

    nominalMetaFeatures = nominalMetaFeatures.filter(col(emptyMF.name) === 0)
      .filter(col("dataType") =!= NumericStr || col("dataType") =!= DateTimeStr)

    // normalization using z-score
    var nomZscore = normalizeDF(nominalMetaFeatures, "nominal")


    val attributesNominal = dfQ.schema.filter(
      a => a.dataType.isInstanceOf[StringType]).map(a => a.name)

    val matchingNom = createPairs(attributesNominal,
      getMetaFeatures("nominal"), nomZscore, currentQueryDataset)


    (nomZscore, matchingNom)

  }

  var k = 4
  var incremental = false
  var demo = false


  def option(key: String, value: Int): FQJ.type = {
    key match {
      case "setK" =>
        k = value
      case _ =>
        // scalastyle:off println
        println("unrecognized option")
      // scalastyle:on println
    }
    this
  }

  def option(key: String, value: String): FQJ.type = {
    key match {
      case "incremental" =>
        if (value =="true") {
          incremental = true
        } else {
         incremental = false
        }
      case "saveComparison" =>
        if (value =="true") {
          demo = true
        } else {
          demo = false
        }
      case _ =>

        // scalastyle:off println
        println("unrecognized option")
      // scalastyle:on println
    }
    this
  }


//  def performJoins(dfNom: DataFrame, dfNum: DataFrame, threshold: Double, dfS: Seq[DataFrame] )
//    : Seq[DataFrame] = {
//
//    var mapSeq = Map(dfS.map(a => a.inputFiles(0).split("/").last -> a ): _*)
//
//    val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))
//    val cols = Seq("ds_name", "att_name", "ds_name_2", "att_name_2", "prob1").map(col)
//    val cols1 = Seq("ds_name", "att_name", "ds_name_2", "att_name_2", "probability").map(col)
//    val dfF = dfNom.select(cols1: _*).union(dfNum.select(cols1: _*))
//    val dfP = dfF.withColumn("prob1", second(col("probability")))
//      .sort(desc("prob1"))
//      .filter(col("prob1") > threshold)
//      .select(cols: _*)
//
//    val reduceD = dfP.dropDuplicates("ds_name", "att_name")
//      .dropDuplicates("ds_name", "att_name_2").sort(desc("prob1"))
//
//    reduceD.show
//
//    var joinsDF: Seq[DataFrame] = Nil
//    reduceD.collect().foreach {
//      case Row( ds_name: String, att_name: String, ds_name_2: String,
//      att_name_2: String, prob1: Double) =>
//
//      val cname = s"${att_name}_${att_name_2}"
//      val cols1 = mapSeq(ds_name).schema.names.map(x =>
//        if (x == att_name) col(att_name).as(cname) else col(x))
//      val cols2 = mapSeq(ds_name_2).schema.names.map(x =>
//        if (x == att_name_2) col(att_name_2).as(cname) else col(x))
//      val m = mapSeq(ds_name).select(cols1: _*).join(mapSeq(ds_name_2).select(cols2: _*), cname )
//      joinsDF = joinsDF :+ m
//    }
//
//    joinsDF
//  }

//  def findJoins(dfSource: DataFrame, dfCandidates: Seq[DataFrame], threshold: Double)
//  : Seq[DataFrame] = {
//
//
//    val tmp = computeDistances(dfSource, dfCandidates)
//    val nomZscore = tmp._1
//    val numZscore = tmp._1
//    val matchingNom = tmp._2
//    val matchingNum = tmp._2
//
//    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/model.zip") )
//
//    val modelRFNum = CrossValidatorModel.load(s"${pathM}/model/numericAtt")
//    val modelRF = CrossValidatorModel.load(s"${pathM}/model/nominalAtt")
//    val predictionNumDF = modelRFNum.transform(matchingNum)
//    val predictionNomDF = modelRF.transform(matchingNom)
//
//    val directory = new Directory(new File(pathM))
//    directory.deleteRecursively()
//
//
//    performJoins(predictionNomDF, predictionNumDF, threshold, dfSource +: dfCandidates)
//
//  }

  def isCached(df: DataFrame): Boolean = df.sparkSession.sharedState.cacheManager
    .lookupCachedData(df.queryExecution.logical).isDefined

//
//  def createPairsContainment(attributes: Seq[String])


  def createPairs(attributes: Seq[String], metaFeatureNames: Seq[MetaFeature],
                      zScoreDF: Dataset[_], fileName: String): Dataset[_] = {

    val columns = zScoreDF.schema.map(_.name)

    val nomAttCandidates = zScoreDF.filter(col("ds_name") =!= fileName)
      .select(columns.map(x => col(x).as(s"${x}_2")): _*).cache()

    // select the normalized meta-features from the dataset source
    // perform a cross join with the attributes from other datasets
    val tmp = zScoreDF.filter(col("ds_name") === fileName).crossJoin(nomAttCandidates)
//    if(incremental && isQueryByAttr) {
//      val cardiQA = zScoreDF.select("bestContainment").take(1).head.getAs("bestContainment")
//
//      // scalastyle:off println
//      println("before QUERYATT")
//      println(nomAttCandidates.count)
//      // scalastyle:on println
//      var nomAttCTmp = nomAttCandidates.withColumn("flippedContainment",
//        flipContainmentUDF(cardiQA,
//          col(s"bestContainment_2").cast(DoubleType))
//      ).withColumn(
//        "worstBestContainment", col("flippedContainment"))
//      k match {
//        case 0 =>
//          nomAttCTmp = nomAttCTmp
//            .filter(col("flippedContainment") <= 0.25)
//        case 1 =>
//          nomAttCTmp = nomAttCTmp
//            .filter(col("flippedContainment") <= 0.25)
//        case 2 =>
//          nomAttCTmp = nomAttCTmp
//            .filter(col("flippedContainment") < 0.5 && col("flippedContainment") >= 0.25)
//        case 3 =>
//          nomAttCTmp = nomAttCTmp
//            .filter(col("flippedContainment") < 0.75 && col("flippedContainment") >= 0.5)
//        case 4 =>
//          nomAttCTmp = nomAttCTmp
//            .filter(col("flippedContainment") >= 0.75)
//      }
//      tmp = zScoreDF.filter(col("ds_name") === fileName).crossJoin(nomAttCTmp)
//
//      // scalastyle:off println
//      println("tmp QUERYATT")
//      println(nomAttCTmp.count)
//      // scalastyle:on println
//
//    }


    // forcing cache of the distances
    var attributesPairs = zScoreDF.sparkSession.createDataFrame(tmp.rdd, tmp.schema).cache()

    attributesPairs = attributesPairs.withColumn("flippedContainment",
      flipContainmentUDF(col("bestContainment").cast(DoubleType),
        col(s"bestContainment_2").cast(DoubleType))
    )
    attributesPairs = attributesPairs.withColumn(
      "worstBestContainment", col("flippedContainment"))


    if(incremental) {
      // scalastyle:off println
      println("before")
      println(attributesPairs.count)
      // scalastyle:on println


      k match {
        case 0 =>
          attributesPairs = attributesPairs
            .filter(col("flippedContainment") <= 0.25)
        case 1 =>
          attributesPairs = attributesPairs
            .filter(col("flippedContainment") <= 0.25)
        case 2 =>
          attributesPairs = attributesPairs
            .filter(col("flippedContainment") < 0.5 && col("flippedContainment") >= 0.25)
        case 3 =>
          attributesPairs = attributesPairs
            .filter(col("flippedContainment") < 0.75 && col("flippedContainment") >= 0.5)
        case 4 =>
          attributesPairs = attributesPairs
            .filter(col("flippedContainment") >= 0.75)
      }
      // scalastyle:off println
      println("after")
      println(attributesPairs.count)
      // scalastyle:on println
    }


    for (metafeature <- metaFeatureNames.filter(_.normalize)) {
      metafeature.normalizeType match {
        case 3 =>
          attributesPairs = attributesPairs.withColumn(metafeature.name,
            containmentCleanUDF(col(metafeature.name), col(s"${metafeature.name}_2")) )
        case 2 =>
          attributesPairs = attributesPairs.withColumn(
            metafeature.name, containmentUDF(col(metafeature.name), col(s"${metafeature.name}_2")) )
        case 1 => // edit distance
          attributesPairs = attributesPairs.withColumn(metafeature.name,
            editDist(lower(trim(col(metafeature.name))),
              lower(trim(col(s"${metafeature.name}_2")))))
        case 5 =>
          // BestContainment
          attributesPairs = attributesPairs.withColumn(
            metafeature.name,
            possContainment(col(metafeature.name).cast(DoubleType),
              col(s"${metafeature.name}_2").cast(DoubleType))
          )
        case _ => // 0 y 4
            attributesPairs = attributesPairs.withColumn(
              metafeature.name, col(metafeature.name) - col(s"${metafeature.name}_2"))

      }
    }
    attributesPairs = attributesPairs.withColumn(
      "name_dist", editDist(lower(trim(col("att_name"))), lower(trim(col("att_name_2"))))
    )
    attributesPairs.drop(metaFeatureNames.filter(_.normalize).map(x => s"${x.name}_2"): _*)
  }



  def possibleContainment(num1: Double, num2: Double): Double = {
    val minN = scala.math.min(num1, num2)

    minN/num1
  }
  def flippedContainment(num1: Double, num2: Double): Double = {
    val minN = scala.math.min(num1, num2)

    minN/scala.math.max(num1, num2)
  }

  lazy val possContainment = udf(possibleContainment(_: Double, _: Double): Double)
  lazy val flipContainmentUDF = udf(flippedContainment(_: Double, _: Double): Double)
  lazy val editDist = udf(levenshtein(_: String, _: String): Double)

//  def levenshtein(s1: String, s2: String): Double = {
//    if (s1 == null || s1 == "") {
//      1
//    } else if (s2 == null || s2 == "") {
//      1
//    } else {
//      val memorizedCosts = Map[(Int, Int), Int]()
//
//      def lev: ((Int, Int)) => Int = {
//        case (k1, k2) =>
//          memorizedCosts.getOrElseUpdate((k1, k2), (k1, k2) match {
//            case (i, 0) => i
//            case (0, j) => j
//            case (i, j) =>
//              ParSeq(1 + lev((i - 1, j)),
//                1 + lev((i, j - 1)),
//                lev((i - 1, j - 1))
//                  + (if (s1(i - 1) != s2(j - 1)) 1 else 0)).min
//          })
//      }
//
//      val leve = lev((s1.length, s2.length))
//      leve/max(s1.length, s2.length).toDouble
//    }
//  }

  def levenshtein(str1: String, str2: String): Double = {
    if (str1 == null || str1 == "") {
      1
    } else if (str2 == null || str2 == "") {
      1
    } else {

      val lenStr1 = str1.length
      val lenStr2 = str2.length

      val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)

      for (i <- 0 to lenStr1) d(i)(0) = i
      for (j <- 0 to lenStr2) d(0)(j) = j

      for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
        val cost = if (str1(i - 1) == str2(j - 1)) 0 else 1

        d(i)(j) = mini(
          d(i-1)(j  ) + 1,     // deletion
          d(i  )(j-1) + 1,     // insertion
          d(i-1)(j-1) + cost   // substitution
        )
      }

      val lev = d(lenStr1)(lenStr2)

      lev.toDouble / scala.math.max(lenStr1, lenStr2).toDouble
    }
  }

  def mini(nums: Int*): Int = nums.min


  val containmentUDF = udf(containment(_: Seq[String], _: Seq[String]): Double)
  def containment(array1: Seq[String], array2: Seq[String]): Double = {

    if (array1.isEmpty || array2.isEmpty) {
      0.0
    } else {
      val inter = array1.intersect(array2).size.toDouble
      val containment1 = inter/array1.size.toDouble
      val containment2 = inter/array2.size.toDouble

      if (containment1 > containment2) containment1 else containment2
    }
  }

  val containmentCleanUDF = udf(containment(_: Seq[String], _: Seq[String]): Double)

  def containmentClean(array1: Seq[String], array2: Seq[String]): Double = {


    val a1 = array1.map(x => Normalizer
      .normalize(x, Normalizer.Form.NFD)
      .replaceAll("[^a-zA-Z\\d]", ""))

    val a2 = array1.map(x => Normalizer
      .normalize(x, Normalizer.Form.NFD)
      .replaceAll("[^a-zA-Z\\d]", ""))

    containment(a1, a2)
  }





}
