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

import scala.collection.mutable.Map
import scala.collection.parallel.ParSeq
import scala.math.max
import scala.reflect.io.Directory

import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql.functions.{abs, col, desc, input_file_name, lit, udf}
import org.apache.spark.sql.types.{NumericType, StringType}
import org.apache.spark.sql.utils.{FindJoinUtils, Unzip}




object findJoins {




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


    val tmp = findJ(dfSource, dfCandidates)
    val nomZscore = tmp._1
    val numZscore = tmp._2
    val matchingNom = tmp._3
    val matchingNum = tmp._4

//    val pathM = getClass.getResource("/model").getPath
//    val pathTemp =
//    val outputFolder = Files.createTempDirectory("modelsFJ");
//
//
//    val a  = ZipFile
//
//    new ZipFile("filename.zip").extractAll("/destination_directory");
    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/model.zip") )
    // scalastyle:off println
    println("********HOLI********")
    println(pathM)
    // scalastyle:on println
                                     
    val modelRFNum = CrossValidatorModel.load(s"${pathM}/model/numericAtt")
    val modelRF = CrossValidatorModel.load(s"${pathM}/model/nominalAtt")
    val predictionNumDF = modelRFNum.transform(matchingNum)
    val predictionNomDF = modelRF.transform(matchingNom)

    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()


    performJoins(predictionNomDF, predictionNumDF, threshold, dfSource +: dfCandidates)


  }

  def findJ(dfSo: Dataset[_], dfSeq: Seq[Dataset[_]]) : (Dataset[_],
    Dataset[_], Dataset[_], Dataset[_]) = {

    var (dsMF, numMF, nomMF) = dfSo.metaFeatures

    // TODO: Make stronger file name to avoid duplications
    val fileName = dfSo.inputFiles(0).split("/").last

    // compute metaFeatures for all datasets
    for (i <- 0 to dfSeq.size-1) {
      var (dsTmp, numTmp, nomTmp) = dfSeq(i).metaFeatures
      nomMF = nomMF.union(nomTmp)
      numMF = numMF.union(numTmp)
      //      dsMF = dsMF.union(dsTmp)
    }

    // scalastyle:off println
    //    println(s"${nomMF.count()}***")
    //    nomMF.show(10)
    // scalastyle:on println

      val nomZscore = FindJoinUtils.standarizeDF(nomMF, "nominal")
    val numZscore = FindJoinUtils.standarizeDF(numMF, "numeric")
    //    StatMetaFeature.standarizeDF(dsMF, "datasets")


    val nominalA = dfSo.logicalPlan.output
      .filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
    val numericA = dfSo.logicalPlan.output
      .filter(a => a.dataType.isInstanceOf[NumericType]).map(a => a.name)

    //    val attributes = dfSo.logicalPlan.output.map(_.name)
    val colsMeta = nomZscore.logicalPlan.output.map(_.name)
    val colsMetaNum = numZscore.logicalPlan.output.map(_.name)
    val matching = matchAtt(nominalA, colsMeta, nomZscore, fileName)
    val matching2 = matchAtt(numericA, colsMetaNum, numZscore, fileName)
    //

    val matchingNom = matching.na.fill(0, matching.logicalPlan.output.map(_.name))
    val matchingNum = matching2.na.fill(0, matching2.logicalPlan.output.map(_.name) )
    //    val nomAttCandidates = nomZscore.filter(col("ds_name") =!= fileName)
    //      .select(colsMeta.map(x => col(x).as(s"${x}_2")): _*)
    //
    //    var matching: DataFrame = null
    //    var flag = true
    //    colsMeta = colsMeta.filter(_ != "ds_name").filter(_ != "att_name")
    //
    //    for (att <- attributes) {
    //      var tmp = nomZscore.filter(col("ds_name") === fileName  && col("att_name") === att)
    //      var matchingTmp = tmp.crossJoin(nomAttCandidates)
    //      // compute distances and merge in the same column
    //      for (c <- colsMeta) {
    //        matchingTmp = matchingTmp.withColumn(c, abs(col(c) - col(s"${c}_2")))
    //      }
    //      matchingTmp = matchingTmp.withColumn(
    //        "name_dist", editDist(col("att_name"), col("att_name_2")))
    //      matchingTmp = matchingTmp.drop(colsMeta.map(x => s"${x}_2"): _*)
    //      if (flag) {
    //        matching = matchingTmp
    //        flag = false
    //      } else {
    //        matching = matching.union(matchingTmp)
    //      }
    //    }

    (nomZscore, numZscore, matchingNom, matchingNum)
  }

  def matchAtt(attributes: Seq[String], cols: Seq[String],
               nomZscore: Dataset[_], fileName: String): Dataset[_] = {
    //    val attributes = dfSo.logicalPlan.output.map(_.name)
    //    var cols = nomZscore.logicalPlan.output.map(_.name)

    val nomAttCandidates = nomZscore.filter(col("ds_name") =!= fileName)
      .select(cols.map(x => col(x).as(s"${x}_2")): _*)

    var matching: DataFrame = null
    var flag = true
    val colsMeta = cols.filter(_ != "ds_name").filter(_ != "att_name")

    for (att <- attributes) {
      var tmp = nomZscore.filter(col("ds_name") === fileName  && col("att_name") === att)
      var matchingTmp = tmp.crossJoin(nomAttCandidates)
      // compute distances and merge in the same column
      for (c <- colsMeta) {
        matchingTmp = matchingTmp.withColumn(c, abs(col(c) - col(s"${c}_2")))
      }
      matchingTmp = matchingTmp.withColumn(
        "name_dist", editDist(col("att_name"), col("att_name_2")))
      matchingTmp = matchingTmp.drop(colsMeta.map(x => s"${x}_2"): _*)
      if (flag) {
        matching = matchingTmp
        flag = false
      } else {
        matching = matching.union(matchingTmp)
      }
    }
    matching
  }

  lazy val editDist = udf(levenshtein(_: String, _: String): Double)

  def levenshtein(s1: String, s2: String): Double = {
    val memorizedCosts = Map[(Int, Int), Int]()

    def lev: ((Int, Int)) => Int = {
      case (k1, k2) =>
        memorizedCosts.getOrElseUpdate((k1, k2), (k1, k2) match {
          case (i, 0) => i
          case (0, j) => j
          case (i, j) =>
            ParSeq(1 + lev((i - 1, j)),
              1 + lev((i, j - 1)),
              lev((i - 1, j - 1))
                + (if (s1(i - 1) != s2(j - 1)) 1 else 0)).min
        })
    }

    val leve = lev((s1.length, s2.length))
    leve/max(s1.length, s2.length).toDouble
  }


}
