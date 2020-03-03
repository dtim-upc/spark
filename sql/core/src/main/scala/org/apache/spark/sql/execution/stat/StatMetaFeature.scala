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

package org.apache.spark.sql.execution.stat

import scala.collection.immutable.{Map => InMap}
import scala.collection.mutable.Map

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.metafeatures.MetaFeatureDataset
import org.apache.spark.sql.types.{LongType, NumericType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object StatMetaFeature extends Logging{

  private val NumericAtt = "numeric"
  private val NominalType = "nominal"
  private val AllType = "all"

  var MetaDataset = new MetaFeatureDataset()

  /*
   * Attributes Meta-features
   */
  private val MeanM = "mean"
  private val StdM = "std"
  private val MinVM = "min_val"
  private val RangeM = "range_val"
  private val CoVarM = "co_of_var"
  private val SizeAvgM = "val_size_avg"
  private val SizeMinM = "val_size_min"
  private val SizeMaxM = "val_size_max"
  private val SizeStdMax = "val_size_std"
  private val PctMinM = "val_pct_min"
  private val PctMaxM = "val_pct_max"
  private val PctStdM = "val_pct_std"
  private val CntDistinct = "distinct_values_cnt"
  private val PctDistinct = "distinct_values_pct"
  private val PctMissing = "missing_values_pct"
  private val PctMedian = "val_pct_median"
  // extra meta-feature
  private val Valmissing = "missing_values"



  private val resA = Map[String, DataFrame]()
  private val resAAll = Map[String, Seq[(String, Long)]]()

  def computeMetaFeature(ds: Dataset[_]): DataFrame = {

    MetaDataset = new MetaFeatureDataset()

    val outputDs = ds.logicalPlan.output
    val defaultMeta = InMap(MeanM -> NumericAtt, StdM -> NumericAtt, MinVM -> NumericAtt,
      RangeM -> NumericAtt, CoVarM -> NumericAtt, SizeAvgM -> NominalType, PctMedian -> NominalType,
      SizeMinM -> NominalType, SizeMaxM -> NominalType, SizeStdMax -> NominalType,
      PctMinM -> NominalType, PctMaxM -> NominalType, PctStdM -> NominalType,
      CntDistinct -> AllType, PctMissing -> AllType, PctDistinct -> AllType)

    // TODO: Check what types are UDTs, arrays, structs, and maps to handle them
    val nominalA = outputDs.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
    val numericA = outputDs.filter(a => a.dataType.isInstanceOf[NumericType]).map(a => a.name)
    val attributes = nominalA ++ numericA

    MetaDataset.numberAttributes = outputDs.size
    MetaDataset.numberInstances = ds.count
    MetaDataset.numberAttNominal = nominalA.size
    MetaDataset.numberAttNumeric = numericA.size
    MetaDataset.dimensionality = MetaDataset.numberAttributes/MetaDataset.numberInstances
    MetaDataset.percAttNominal = MetaDataset.numberAttNominal*100/MetaDataset.numberAttributes
    MetaDataset.percAttNumeric = MetaDataset.numberAttNumeric*100/MetaDataset.numberAttributes

    MetaDataset.avgNominal = aggregateDatasetMeta(avg, ds, CntDistinct, nominalA)
    MetaDataset.avgNumeric = aggregateDatasetMeta(avg, ds, MeanM, numericA)

    MetaDataset.stdNominal = aggregateDatasetMeta(stddev, ds, CntDistinct, nominalA)
    MetaDataset.stdNumeric = aggregateDatasetMeta(stddev, ds, MeanM, numericA)

    MetaDataset.minNominal = aggregateDatasetMeta(min, ds, CntDistinct, nominalA)
    MetaDataset.maxNominal = aggregateDatasetMeta(max, ds, CntDistinct, nominalA)

    MetaDataset.minNumeric = aggregateDatasetMeta(min, ds, MeanM, numericA)
    MetaDataset.maxNumeric = aggregateDatasetMeta(max, ds, MeanM, numericA)

    MetaDataset.minMissing = aggregateDatasetMeta(min, ds, Valmissing, attributes)
    MetaDataset.maxMissing = aggregateDatasetMeta(max, ds, Valmissing, attributes)

    MetaDataset.minMissingPerc = aggregateDatasetMeta(min, ds, PctMissing, attributes)
    MetaDataset.maxMissingPerc = aggregateDatasetMeta(max, ds, PctMissing, attributes)

    MetaDataset.meanMissing = aggregateDatasetMeta(mean, ds, Valmissing, attributes)
    MetaDataset.meanMissingPerc = aggregateDatasetMeta(mean, ds, Valmissing, attributes)

    val dataF = attributes.map(x => resA.get(Valmissing).get.select(col(x).as("id")))
    MetaDataset.missingAttCnt = dataF.reduce(_.union(_)).select(count(when(col("id") > 0, true))
      .cast("double")).first().get(0).asInstanceOf[Double]

    MetaDataset.missingAttPerc = dataF.reduce(_.union(_)).select((count(when(col("id") > 0, true))
      *100/MetaDataset.numberAttributes).cast("double")).first().get(0).asInstanceOf[Double]

    // scalastyle:off println
    println("***")
    println(s"$MetaDataset")
    // scalastyle:on println

    val result = Array.fill[InternalRow](defaultMeta.size)
      {new GenericInternalRow(attributes.length + 1)}

    // TODO: reset map
    var rowIndex = 0
    for ((k, v) <- defaultMeta) {
      result(rowIndex).update(0, UTF8String.fromString(k))
      val selectCols = getColumns(v, numericA, nominalA, attributes)
      val statDf = resA.getOrElseUpdate(k, getMeta(k, ds, selectCols))
      for ((x, i) <- attributes.view.zipWithIndex) {
        val statVal = if (selectCols.contains(x)) statDf.select(x).first().get(0) + "" else null
        result(rowIndex).update(i + 1, UTF8String.fromString(statVal))
      }
      rowIndex += 1
    }
    val output = AttributeReference("metaFeature", StringType)() +:
      attributes.map(a => AttributeReference(a.toString, StringType)())
    Dataset.ofRows(ds.sparkSession, LocalRelation(output, result))
  }

  private def getColumns(k: String, num: Seq[String], nom: Seq[String], all: Seq[String])
    : Seq[String] = k match {
    case NumericAtt => num
    case NominalType => nom
    case AllType => all
  }


  private def getMeta(meta: String, ds: Dataset[_], cols: Seq[String]): DataFrame = meta match {
    case MeanM => aggregateNumericMeta(mean, ds, cols)
    case StdM => aggregateNumericMeta(stddev, ds, cols)
    case MinVM => aggregateNumericMeta(min, ds, cols)
    case RangeM =>
      val maxDf = ds.select(cols.map( x => max(x).as(s"${x}_" + RangeM)): _*)
      val minDf = resA.getOrElseUpdate(MinVM, getMeta(MinVM, ds, cols))
      maxDf.crossJoin(minDf).select(cols.map(x =>
        concat(col(x), lit(", "), col(s"${x}_" + RangeM)).as(x)): _*)
    case CoVarM =>
      val meanDf = resA.getOrElseUpdate(MeanM, getMeta(MeanM, ds, cols))
      val std = resA.getOrElseUpdate(StdM, getMeta(MeanM, ds, cols))
      meanDf.select(cols.map(x => col(x).as(s"${x}_" + MeanM)): _*).crossJoin(std).
        select(cols.map(x => (col(x)/col(s"${x}_" + MeanM)).as(x)): _*)
    case SizeAvgM => aggregateNominalMeta(avg, ds, cols, false)
    case SizeMinM => aggregateNominalMeta(min, ds, cols, false)
    case SizeMaxM => aggregateNominalMeta(max, ds, cols, false)
    case SizeStdMax => aggregateNominalMeta(stddev, ds, cols, false)
    case PctMinM => aggregateNominalMeta(min, ds, cols, true)
    case PctMaxM => aggregateNominalMeta(max, ds, cols, true)
    case PctStdM => aggregateNominalMeta(stddev, ds, cols, true)
    case PctMedian =>
      val dfTmp = getMeta(CntDistinct, ds, cols).select(cols.map(col(_).cast("double")): _*)
      val data = cols.map(x => x -> dfTmp.stat.approxQuantile(x, Array(0.5), 0.25)(0).longValue())
      createDF(ds, data)
    case CntDistinct => createDF(ds, cols.map(x => x -> ds.select(col(x).cast("double"))
      .distinct.count))
    case PctDistinct => resA.getOrElseUpdate(CntDistinct, getMeta(CntDistinct, ds, cols)).
      select(cols.map(x => (col(x)*100/MetaDataset.numberInstances).as(x)): _*)
    case PctMissing => ds.select(cols.map(c => (sum(col(c).isNull
      .cast("int"))*100/MetaDataset.numberInstances).as(c)): _*)
    case Valmissing => ds.select(cols.map(c => sum(col(c).isNull.cast("int")).as(c)): _*)
    case _ => throw new IllegalArgumentException(s"$meta is not a recognised meta")
  }


  private def createDF(ds: Dataset[_], data: Seq[(String, Long)]): DataFrame = {
    val res = Array.fill[InternalRow](1) {new GenericInternalRow(data.length)}
    for ((x, i) <- data.view.zipWithIndex) res(0).update(i, UTF8String.fromString(x._2 + ""))
    val output = data.map(a => AttributeReference(a._1.toString, StringType)())
    Dataset.ofRows(ds.sparkSession, LocalRelation(output, res))
  }

  def aggregateNumericMeta(f: String => Column, ds: Dataset[_], cols: Seq[String]): DataFrame = {
    ds.select(cols.map( x => f(x).as(x)): _*)
  }

  def aggregateDatasetMeta(f: String => Column, ds: Dataset[_], meta: String, cols: Seq[String])
    : Double = {
    val df = meta match {
      case CntDistinct => resA.getOrElseUpdate(meta, getMeta(meta, ds, ds.columns)).
           select(cols.map(col(_).cast("double")): _*)
      case _ => resA.getOrElseUpdate(meta, getMeta(meta, ds, cols))
    }
    cols.map(x => df.select(col(x).as(meta))).reduce(_.union(_))
      .select(f(meta).cast("double")).first().get(0).asInstanceOf[Double]
  }

  def aggregateNominalMeta(f: Column => Column, ds: Dataset[_], cols: Seq[String],
                           perc: Boolean): DataFrame = {
    if (perc) {
      ds.columns.map(x => ds.groupBy(x).count().select(
        max(col("count")*100/MetaDataset.numberInstances).as(x))).reduce((a, b) => a.crossJoin(b))
    } else {
      ds.columns.map(x => ds.groupBy(x).count().
        select(f(col("count")).as(x))).reduce((a, b) => a.crossJoin(b))
    }
  }

}
