///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.utils
//
//import scala.collection.immutable.{Map => InMap}
//import scala.collection.mutable.Map
//
//import org.apache.spark.sql.{DataFrame, Dataset}
//import org.apache.spark.sql.functions.{col, lit, mean, monotonically_increasing_id, stddev, udf}
//// import org.apache.spark.sql.metafeatures.MetaFeatureDatasets2
//
//object FindJoinUtils {
//
//  private val NumericAtt = "numeric"
//  private val NominalType = "nominal"
//  private val AllType = "all"
//
//  private val MeanM = "mean"
//  private val StdM = "std"
//  private val MinVM = "min_val"
//  private val MaxVM = "max_val"
//  private val RangeM = "range_val"
//  private val CoVarM = "co_of_var"
//  private val SizeAvgM = "val_size_avg"
//  private val SizeMinM = "val_size_min"
//  private val SizeMaxM = "val_size_max"
//  private val SizeStdMax = "val_size_std"
//  private val SizeCoVarMax = "val_size_co_of_var"
//  private val PctMinM = "val_pct_min"
//  private val PctMaxM = "val_pct_max"
//  private val PctStdM = "val_pct_std"
//  private val CntDistinct = "distinct_values_cnt"
//  private val PctDistinct = "distinct_values_pct"
//  private val PctMissing = "missing_values_pct"
//  private val PctMedian = "val_pct_median"
//  // extra meta-feature
//  private val Valmissing = "missing_values"
//
//
//  private var resA = Map[String, DataFrame]()
//
//  val ColAtt = "att_name"
//
//  val AllMeta = InMap(CntDistinct -> AllType, PctMissing -> AllType, PctDistinct -> AllType)
//  val NumericMeta = AllMeta ++ InMap(MeanM -> NumericAtt, StdM -> NumericAtt, MinVM -> NumericAtt,
//    MaxVM -> NumericAtt, RangeM -> NumericAtt, CoVarM -> NumericAtt)
//  val NominalMeta = AllMeta ++ InMap(SizeAvgM -> NominalType, PctMedian -> NominalType,
//    SizeMinM -> NominalType, SizeMaxM -> NominalType, SizeCoVarMax -> NominalType,
//    PctMinM -> NominalType, PctMaxM -> NominalType, PctStdM -> NominalType)
//  /**
//   *
//   * @param df need to have colum ds_name and att_name
//   * @param metaType
//   * @return
//   */
//  def normalizeDF(df: Dataset[_], metaType: String): DataFrame = {
////    val zScoreUDF = udf(zScore(_: Double, _: Double, _: Double): Double)
//    val cols = getColumnsNamesMetaFeatures(metaType)
//
//    var meanAndStdDF = cols.map(x => df.select(mean(col(x)).as(s"${x}_avg"),
//      stddev(col(x)).as(s"${x}_std"))
//      .withColumn("id", monotonically_increasing_id))
//      .reduce(_.join(_, "id"))
//
//    meanAndStdDF.show
//
//    val meanAndStd = meanAndStdDF.take(1).head
//    var zScoreDF = df
//    for (c <- cols) {
//      zScoreDF = zScoreDF.withColumn(c,
//        (col(c) - lit(meanAndStd.getAs(s"${c}_avg"))) /  lit(meanAndStd.getAs(s"${c}_std")))
//    }
//    val dataF = zScoreDF.toDF()
//    df.sparkSession.createDataFrame(dataF.rdd, dataF.schema).cache()
//
//  }
//
////  private def zScore(x: Double, avgVal: Double, stdVal: Double): Double = {
////    (x - avgVal) / stdVal
////  }
//
//  def getColumnsNamesMetaFeatures(metaType: String): Seq[String] = metaType match {
//    case "numeric" => NumericMeta.keySet.toSeq
//    case "nominal" => NominalMeta.keySet.toSeq
////    case "datasets" => MetaFeatureDatasets2.cols()
//    case _ => NominalMeta.keySet.toSeq
//  }
//}
//
//
