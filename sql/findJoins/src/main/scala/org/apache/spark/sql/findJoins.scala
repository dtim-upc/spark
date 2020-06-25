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
import org.apache.spark.sql.types.{NumericType, StringType, DoubleType}
import org.apache.spark.sql.utils.Unzip

object findJoins extends  Logging {

  def performJoins(dfNom: DataFrame, dfNum: DataFrame, threshold: Double, dfS: Seq[DataFrame] )
    : Seq[DataFrame] = {

    var mapSeq = Map(dfS.map(a => a.inputFiles(0).split("/").last -> a ): _*)

    val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))
    val cols = Seq("ds_name", "att_name", "ds_name_2", "att_name_2", "prob1").map(col)
    val cols1 = Seq("ds_name", "att_name", "ds_name_2", "att_name_2", "probability").map(col)
    val dfF = dfNom.select(cols1: _*).union(dfNum.select(cols1: _*))
    val dfP = dfF.withColumn("prob1", second(col("probability")))
      .sort(desc("prob1"))
      .filter(col("prob1") > threshold)
      .select(cols: _*)

    val reduceD = dfP.dropDuplicates("ds_name", "att_name")
      .dropDuplicates("ds_name", "att_name_2").sort(desc("prob1"))

    reduceD.show

    var joinsDF: Seq[DataFrame] = Nil
    reduceD.collect().foreach {
      case Row( ds_name: String, att_name: String, ds_name_2: String,
      att_name_2: String, prob1: Double) =>

      val cname = s"${att_name}_${att_name_2}"
      val cols1 = mapSeq(ds_name).schema.names.map(x =>
        if (x == att_name) col(att_name).as(cname) else col(x))
      val cols2 = mapSeq(ds_name_2).schema.names.map(x =>
        if (x == att_name_2) col(att_name_2).as(cname) else col(x))
      val m = mapSeq(ds_name).select(cols1: _*).join(mapSeq(ds_name_2).select(cols2: _*), cname )
      joinsDF = joinsDF :+ m
    }

    joinsDF
  }


  def findJoins(dfSource: DataFrame, dfCandidates: Seq[DataFrame], threshold: Double)
  : Seq[DataFrame] = {


    val tmp = computeDistances(dfSource, dfCandidates)
    val nomZscore = tmp._1
    val numZscore = tmp._1
    val matchingNom = tmp._2
    val matchingNum = tmp._2

    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/model.zip") )
                                     
    val modelRFNum = CrossValidatorModel.load(s"${pathM}/model/numericAtt")
    val modelRF = CrossValidatorModel.load(s"${pathM}/model/nominalAtt")
    val predictionNumDF = modelRFNum.transform(matchingNum)
    val predictionNomDF = modelRF.transform(matchingNom)

    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()


    performJoins(predictionNomDF, predictionNumDF, threshold, dfSource +: dfCandidates)

  }

  def isCached(df: DataFrame): Boolean = df.sparkSession.sharedState.cacheManager
    .lookupCachedData(df.queryExecution.logical).isDefined

  def computeDistances(dfSource: DataFrame, dfCandidates: Seq[DataFrame], norm: Boolean = true,
                       direction: Boolean = false): (Dataset[_], Dataset[_]) = {
    var (numericMetaFeatures, nominalMetaFeatures) = dfSource.metaFeatures

    val fileName = dfSource.inputFiles(0).split("/").last

    // compute metaFeatures for all datasets and merge them in two dataframes: nominal and numeric
    for (i <- 0 to dfCandidates.size-1) {
      var (numericMetaFeaturesTmp, nominalMetaFeaturesTmp) = dfCandidates(i).metaFeatures
//      if (!numericMetaFeaturesTmp.head(1).isEmpty) {
//        numericMetaFeatures = numericMetaFeatures.union(numericMetaFeaturesTmp)
//      }
      if (!nominalMetaFeaturesTmp.head(1).isEmpty) {
        nominalMetaFeatures = nominalMetaFeatures.union(nominalMetaFeaturesTmp)
      }
    }


    nominalMetaFeatures = nominalMetaFeatures.filter(col(emptyMF.name) === 0)
//    numericMetaFeatures = numericMetaFeatures.filter(col(emptyMF.name) === 0)

      // normalization using z-score
    val nomZscore = if (norm) normalizeDF(nominalMetaFeatures, "nominal") else nominalMetaFeatures
//    val numZscore = if (norm) normalizeDF(numericMetaFeatures, "numeric") else numericMetaFeatures

    val attributesNominal = dfSource.schema.filter(
      a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
//    val attributesNumeric = dfSource.schema.filter(
//      a => a.dataType.isInstanceOf[NumericType]).map(a => a.name)

    val matchingNom = createPairs(attributesNominal,
      getMetaFeatures("nominal"), nomZscore, fileName, direction)
//    val matchingNum = createPairs(attributesNumeric,
//      getMetaFeatures("numeric"), numZscore, fileName)

    (nomZscore, matchingNom)
  }


  def createPairs(attributes: Seq[String], metaFeatureNames: Seq[MetaFeature], zScoreDF: Dataset[_],
                  fileName: String, direction: Boolean): Dataset[_] = {

    val columns = zScoreDF.schema.map(_.name)

    val nomAttCandidates = zScoreDF.filter(col("ds_name") =!= fileName)
      .select(columns.map(x => col(x).as(s"${x}_2")): _*).cache()

    // select the normalized meta-features from the dataset source
    // perform a cross join with the attributes from other datasets
    val tmp = zScoreDF.filter(col("ds_name") === fileName).crossJoin(nomAttCandidates)
    // forcing cache of the distances
    var attributesPairs = zScoreDF.sparkSession.createDataFrame(tmp.rdd, tmp.schema).cache()

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
          if (!direction) {
            attributesPairs = attributesPairs.withColumn(
              metafeature.name, abs(col(metafeature.name) - col(s"${metafeature.name}_2")))
          } else {
            attributesPairs = attributesPairs.withColumn(
              metafeature.name, col(metafeature.name) - col(s"${metafeature.name}_2"))
          }
      }
//      attributesPairs = attributesPairs.withColumn(
//        metafeature.name, abs(col(metafeature.name) - col(s"${metafeature.name}_2")))
    }
    // compute the distance name from the two attributes pair
    attributesPairs = attributesPairs.withColumn(
      "name_dist", editDist(lower(trim(col("att_name"))), lower(trim(col("att_name_2"))))
    )

    // remove the meta-features columns from the second attribute pair
    attributesPairs.drop(metaFeatureNames.filter(_.normalize).map(x => s"${x.name}_2"): _*)
  }


  def possibleContainment(num1: Double, num2: Double): Double = {
    val minN = scala.math.min(num1, num2)

    minN/num1
  }

  lazy val possContainment = udf(possibleContainment(_: Double, _: Double): Double)
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
