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

package org.apache.spark.sql.findJoinsP

import scala.collection.mutable.Map
import scala.collection.parallel.ParSeq
import scala.math.max

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.stat.StatMetaFeature
import org.apache.spark.sql.functions.{abs, col, input_file_name, lit, udf}

object findJoinsObj {




  def findJ(dfSo: Dataset[_], dfSeq: Seq[Dataset[_]]) : (Dataset[_], Dataset[_], DataFrame) = {

    var (dsMF, numMF, nomMF) = dfSo.metaFeatures

    // TODO: Make stronger file name to avoid duplications
    nomMF = nomMF
    numMF = numMF
    val fileName = nomMF.select("ds_name").first().get(0).asInstanceOf[String]

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

    val nomZscore = StatMetaFeature.standarizeDF(nomMF, "nominal")
    val numZscore = StatMetaFeature.standarizeDF(numMF, "numeric")
    //    StatMetaFeature.standarizeDF(dsMF, "datasets")

    val attributes = dfSo.logicalPlan.output.map(_.name)
    var colsMeta = nomZscore.logicalPlan.output.map(_.name)

    val nomAttCandidates = nomZscore.filter(col("ds_name") =!= fileName)
      .select(colsMeta.map(x => col(x).as(s"${x}_2")): _*)

    var matching: DataFrame = null
    var flag = true
    colsMeta = colsMeta.filter(_ != "ds_name").filter(_ != "att_name")

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

    (nomZscore, numZscore, matching)
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
