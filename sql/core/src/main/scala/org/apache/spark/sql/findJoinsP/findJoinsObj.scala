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

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.stat.StatMetaFeature
import org.apache.spark.sql.functions.{abs, col, input_file_name, lit, udf}

object findJoinsObj {

  lazy val fileNameDF = udf(get_only_file_name(_: String): String)


  def findJ(dfOriginal: Dataset[_], dfSeq: Seq[Dataset[_]]) : (DataFrame, DataFrame, DataFrame) = {

    var (dsMF, numMF, nomMF) = dfOriginal.metaFeatures

    // TODO: Make stronger file name to avoid duplications
    nomMF = nomMF.withColumn("ds_name", fileNameDF(input_file_name))
    numMF = numMF.withColumn("ds_name", fileNameDF(input_file_name))
    val fileName = nomMF.select("ds_name").first().get(0).asInstanceOf[String]

    // compute metaFeatures for all datasets
    for (i <- 0 to dfSeq.size-1) {
      var (dsTmp, numTmp, nomTmp) = dfSeq(i).metaFeatures
      nomMF = nomMF.union(nomTmp.withColumn("ds_name", fileNameDF(input_file_name)))
      numMF = numMF.union(numTmp.withColumn("ds_name", fileNameDF(input_file_name)))
      //      dsMF = dsMF.union(dsTmp)
    }

    var nomZscore = StatMetaFeature.standarizeDF(nomMF, "nominal")
    var numZscore = StatMetaFeature.standarizeDF(numMF, "numeric")
    //    StatMetaFeature.standarizeDF(dsMF, "datasets")

    val attributes = dfOriginal.logicalPlan.output.map(_.name)
    nomZscore.printSchema()
    var colsMeta = nomZscore.logicalPlan.output.map(_.name)

    val nomAttCandidates = nomZscore.filter(col("ds_name") =!= fileName)
      .select(colsMeta.map(x => col(x).as(s"${x}_2")): _*)

    var matching = nomZscore
    var flag = true
    colsMeta = colsMeta.filter(_ != "ds_name").filter(_ != "att_name")

    for (att <- attributes) {
      var tmp = nomZscore.filter(col("ds_name") === fileName  && col("att_name") === att)
      var matchingTmp = tmp.crossJoin(nomAttCandidates)
      // compute distances and merge in the same column
      for (c <- colsMeta) {
        matchingTmp = matchingTmp.withColumn(c, abs(col(c) - col(s"${c}_2")))
      }
      if (flag) {
        matching = matchingTmp
        flag = false
      } else {
        matching = matching.union( matchingTmp.drop(colsMeta.map(x => s"${x}_2"): _*))
      }
    }

    (nomZscore, numZscore, matching)
  }


  def get_only_file_name(fullPath: String): String = fullPath.split("/").last
}
