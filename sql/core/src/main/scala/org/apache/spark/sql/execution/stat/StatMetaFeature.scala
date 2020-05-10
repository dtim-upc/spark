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
import scala.collection.parallel.ParSeq

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.metafeatures.{MetaFeatureDataset, MetaFeatureDatasets}
import org.apache.spark.sql.types.{LongType, NumericType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object StatMetaFeature extends Logging{

  // TODO: (JAV) change class location
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
  private val MaxVM = "max_val"
  private val RangeM = "range_val"
  private val CoVarM = "co_of_var"
  private val SizeAvgM = "val_size_avg"
  private val SizeMinM = "val_size_min"
  private val SizeMaxM = "val_size_max"
  private val SizeStdMax = "val_size_std"
  private val SizeCoVarMax = "val_size_co_of_var"
  private val PctMinM = "val_pct_min"
  private val PctMaxM = "val_pct_max"
  private val PctStdM = "val_pct_std"
  private val CntDistinct = "distinct_values_cnt"
  private val PctDistinct = "distinct_values_pct"
  private val PctMissing = "missing_values_pct"
  private val PctMedian = "val_pct_median"
  private val Empty = "isEmpty" // when columns have 100% missing values
  // extra meta-feature
  private val Valmissing = "missing_values"


  private var resA = Map[String, DataFrame]()

  val ColAtt = "att_name"

  val AllMeta = InMap(CntDistinct -> AllType, PctMissing -> AllType, PctDistinct -> AllType)
  val NumericMeta = AllMeta ++ InMap(MeanM -> NumericAtt, StdM -> NumericAtt, MinVM -> NumericAtt,
    MaxVM -> NumericAtt, RangeM -> NumericAtt, CoVarM -> NumericAtt)
  val NominalMeta = AllMeta ++ InMap(SizeAvgM -> NominalType, PctMedian -> NominalType,
    SizeMinM -> NominalType, SizeMaxM -> NominalType, SizeCoVarMax -> NominalType,
    PctMinM -> NominalType, PctMaxM -> NominalType, PctStdM -> NominalType)

  def computeMetaFeature(ds: Dataset[_]): (DataFrame, DataFrame, DataFrame) = {

    MetaDataset = new MetaFeatureDataset()
    resA = Map[String, DataFrame]()

    val outputDs = ds.logicalPlan.output
    val defaultMeta = NumericMeta ++ NominalMeta

    // TODO: Check what types are UDTs, arrays, structs, and maps to handle them
    val nominalA = outputDs.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
    val numericA = outputDs.filter(a => a.dataType.isInstanceOf[NumericType]).map(a => a.name)
    val attributes = nominalA ++ numericA

    MetaDataset.numberAttributes = outputDs.size.toDouble
    MetaDataset.numberInstances = ds.count.toDouble
    MetaDataset.numberAttNominal = nominalA.size.toDouble
    MetaDataset.numberAttNumeric = numericA.size.toDouble

    MetaDataset.dimensionality = MetaDataset.numberAttributes/MetaDataset.numberInstances
    MetaDataset.percAttNominal = MetaDataset.numberAttNominal*100/MetaDataset.numberAttributes
    MetaDataset.percAttNumeric = MetaDataset.numberAttNumeric*100/MetaDataset.numberAttributes

    MetaDataset.avgNominal = aggregateDatasetMeta(avg, ds, CntDistinct, nominalA)
    MetaDataset.avgNumeric = aggregateDatasetMeta(avg, ds, MeanM, numericA)

    MetaDataset.stdNominal = aggregateDatasetMeta(stddev_pop, ds, CntDistinct, nominalA)
    MetaDataset.stdNumeric = aggregateDatasetMeta(stddev_pop, ds, MeanM, numericA)

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


    // TODO: Handle when more files and save metadata and when is null!!!
    val filename = ds.inputFiles(0).split("/").last
    val path = ds.inputFiles(0)

    val nomPath = pathMF(path, filename, "nominal")
    val numPath = pathMF(path, filename, "numeric")

    val dsMeta = createDF(ds, MetaDataset.toSeq)
    try {
      val nominalAtt = ds.sparkSession.read.load(nomPath)
      val numericAtt = ds.sparkSession.read.load(numPath)
      // scalastyle:off println
       println("READ META")
        // scalastyle:on println
      (dsMeta, numericAtt, nominalAtt)
    } catch {
      case e:
        // scalastyle:off println
        AnalysisException => println("Couldn't find that file.")
        // scalastyle:on println
        // compute content meta-features
        for ((k, v) <- defaultMeta) {
          val selectCols = getColumns(v, numericA, nominalA, attributes)
          if(selectCols.size > 0) {
            resA.getOrElseUpdate(k, getMeta(k, ds, selectCols))
          }
        }

        val numericAtt = Dataset.ofRows(ds.sparkSession,
          createMFRel(NumericMeta, numericA, filename))
        val nominalAtt = Dataset.ofRows(ds.sparkSession,
          createMFRel(NominalMeta, nominalA, filename))

        val numeric = numericAtt.withColumn(Empty, when(col(PctMissing) === 100, 1).otherwise(0))
        val nominal = nominalAtt.withColumn(Empty, when(col(PctMissing) === 100, 1).otherwise(0))

        saveMF(nomPath, nominal)
        saveMF(numPath, numeric)
        (dsMeta, ds.sparkSession.read.load(numPath), ds.sparkSession.read.load(nomPath))
      case _:
        // TODO: handle it@
        // scalastyle:off println
        Throwable => println("Got some other kind of Throwable exception")
        // scalastyle:on println
        (dsMeta, dsMeta, dsMeta)
    }

  }

  def pathMF(path: String, fn1: String, meta: String): String = {
    val fn = fn1.replace(".", "")
    path.replace(fn1, s".${fn}/.${meta}")
  }

  def saveMF(path: String, ds: Dataset[_]): Unit = {
    ds.write.mode(SaveMode.Overwrite).format("parquet").save(path)
  }

  def createMFRel(meta: InMap[String, String], attSeq: Seq[String], f: String): LocalRelation = {
    val rowsSize = attSeq.length
    val result = Array.fill[InternalRow](rowsSize)
      {new GenericInternalRow(meta.size + 2)} // for attName and ds_name

    var rowIndex = 0
    val columnsOutput = meta.keySet
    for (att <- attSeq) {
          // compute content-level metadata
          result(rowIndex).update(0, UTF8String.fromString(f))
          result(rowIndex).update(1, UTF8String.fromString(att))
          var colIndex = 2
          for ((k, v) <- meta) {
            val stat = resA.get(k).get
            val statVal = stat.select(att).first().get(0) + ""
            result(rowIndex).update(colIndex, UTF8String.fromString(statVal))
            colIndex += 1
          }
          rowIndex += 1
    }
    val output = AttributeReference(MetaFeatureDatasets.ColDs, StringType)() +:
          AttributeReference(ColAtt, StringType)() +:
          columnsOutput.toSeq.map(a => AttributeReference(a, StringType)())
    LocalRelation(output, result)
  }

  private def getColumns(k: String, num: Seq[String], nom: Seq[String], all: Seq[String])
    : Seq[String] = k match {
    case NumericAtt => num
    case NominalType => nom
    case AllType => all
  }


  def getMeta(meta: String, ds: Dataset[_], cols: Seq[String]): DataFrame = meta match {
    case MeanM => aggregateNumericMeta(mean, ds, cols)
    case StdM => aggregateNumericMeta(stddev_pop, ds, cols)
    case MinVM => aggregateNumericMeta(min, ds, cols)
    case MaxVM => aggregateNumericMeta(max, ds, cols)
    case RangeM =>
      var maxDf = ds.select(cols.map( x => max(x).as(s"${x}_" + MaxVM)): _*)
      var minDf = resA.getOrElseUpdate(MinVM, getMeta(MinVM, ds, cols))
      maxDf = maxDf.withColumn("id", monotonically_increasing_id)
      minDf = minDf.withColumn("id", monotonically_increasing_id)
      val cross = minDf.join(maxDf, "id")
        .select( cols.map(x => (col(s"${x}_" + MaxVM)-col(x)).as(x)): _* )
      val newDF = ds.sparkSession.createDataFrame(cross.rdd, cross.schema).cache()
      newDF
    case CoVarM =>
      var meanDf = resA.getOrElseUpdate(MeanM, getMeta(MeanM, ds, cols))
      val std = resA.getOrElseUpdate(StdM, getMeta(StdM, ds, cols))
      meanDf = meanDf.select(cols.map(x => col(x).as(s"${x}_" + MeanM)): _*).crossJoin(std).
        select(cols.map(x => (col(x)/col(s"${x}_" + MeanM)).as(x)): _*)
      val newDF = ds.sparkSession.createDataFrame(meanDf.rdd, meanDf.schema).cache()
      newDF
    case SizeAvgM => aggregateNominalMeta(avg, ds, cols, false)
    case SizeMinM => aggregateNominalMeta(min, ds, cols, false)
    case SizeMaxM => aggregateNominalMeta(max, ds, cols, false)
    case SizeStdMax => aggregateNominalMeta(stddev_pop, ds, cols, false)
    case SizeCoVarMax =>

      var meanDf = resA.getOrElseUpdate(SizeAvgM, getMeta(SizeAvgM, ds, cols))
      val std = resA.getOrElseUpdate(SizeStdMax, getMeta(SizeStdMax, ds, cols))

      meanDf = meanDf.select(cols.map(x => col(x).as(s"${x}_" + MeanM)): _*).crossJoin(std).
        select(cols.map(x => (col(x)/col(s"${x}_" + MeanM)).as(x)): _*)
      val newDF = ds.sparkSession.createDataFrame(meanDf.rdd, meanDf.schema).cache()
      newDF
    case PctMinM => aggregateNominalMeta(min, ds, cols, true)
    case PctMaxM => aggregateNominalMeta(max, ds, cols, true)
    case PctStdM => aggregateNominalMeta(stddev_pop, ds, cols, true)
    case PctMedian =>
      val dfTmp = getMeta(CntDistinct, ds, cols).select(cols.map(col(_).cast("double")): _*)
      val data = cols.map(x => x -> dfTmp.stat.approxQuantile(x, Array(0.5), 0.25)(0).longValue())
      createDF(ds, data)
    case CntDistinct => createDF(ds, cols.map(x => x -> ds.select(col(x)).na.drop
      .distinct.count))
      // TODO: change for agg someDF.select(col("project")).agg(countDistinct("project")).show
    case PctDistinct =>
      val df = resA.getOrElseUpdate(CntDistinct, getMeta(CntDistinct, ds, cols)).
      select(cols.map(x => (col(x)/MetaDataset.numberInstances).as(x)): _*)
      val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
      newDF
    case PctMissing =>
      val df = ds.select(cols.map(c => (sum(col(c).isNull
      .cast("int"))*100/MetaDataset.numberInstances).as(c)): _*)
      val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
      newDF
    case Valmissing =>
      val df = ds.select(cols.map(c => sum(col(c).isNull.cast("int")).as(c)): _*)
      val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
      newDF
    case _ => throw new IllegalArgumentException(s"$meta is not a recognised meta")
  }


  private def createDF(ds: Dataset[_], data: Seq[(String, Long)]): DataFrame = {
    val res = Array.fill[InternalRow](1) {new GenericInternalRow(data.length)}
    for ((x, i) <- data.view.zipWithIndex) res(0).update(i, UTF8String.fromString(x._2 + ""))
    val output = data.map(a => AttributeReference(a._1.toString, StringType)())
    Dataset.ofRows(ds.sparkSession, LocalRelation(output, res))
  }

  def aggregateNumericMeta(f: String => Column, ds: Dataset[_], cols: Seq[String]): DataFrame = {
    val df = ds.select(cols.map( x => f(x).as(x)): _*)
    val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
    newDF

  }

  def aggregateDatasetMeta(f: String => Column, ds: Dataset[_], meta: String, cols: Seq[String])
    : Double = {
    if (cols.isEmpty) {
      0
    } else {
      val df = meta match {
        case CntDistinct => resA.getOrElseUpdate(meta, getMeta(meta, ds, ds.columns)).
          select(cols.map(col(_).cast("double")): _*)
        case _ => resA.getOrElseUpdate(meta, getMeta(meta, ds, cols))
      }
      cols.map(x => df.select(col(x).as(meta))).reduce(_.union(_))
        .select(f(meta).cast("double")).first().get(0).asInstanceOf[Double]
    }

  }

  def aggregateNominalMeta(f: Column => Column, ds: Dataset[_], cols: Seq[String],
                           perc: Boolean): DataFrame = {
    if (perc) {
      val df = cols.map(x => ds.select(x).na.drop.groupBy(x).count().select(
        f(col("count")*100/MetaDataset.numberInstances).as(x))).reduce((a, b) => a.crossJoin(b))

      val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
      newDF
    } else {
      val df = cols.map(x => ds.select(x).na.drop.groupBy(x).count().
        select(f(col("count")).as(x))).reduce((a, b) => a.crossJoin(b))
      val newDF = ds.sparkSession.createDataFrame(df.rdd, df.schema).cache()
      newDF
    }
  }





  /**
   *
   * @param df need to have colum ds_name and att_name
   * @param metaType
   * @return
   */
  def standarizeDF(df: Dataset[_], metaType: String): Dataset[_] = {
    val zScoreUDF = udf(zScore(_: Double, _: Double, _: Double): Double)
    val cols = getColumnsMeta(metaType)

    val meanAndStd = cols.map(x => df.select(mean(col(x)).as(s"${x}_avg"),
      stddev_pop(col(x)).as(s"${x}_std"))
      .withColumn("id", monotonically_increasing_id))
      .reduce(_.join(_, "id")).take(1).head
    // scalastyle:off println
//    println(s"${metaType}***")
//    cols.map(println)
//    println(meanAndStd.toString)
    // scalastyle:on println

    var zScoreDF = df
    for (c <- cols) {
      zScoreDF = zScoreDF.withColumn(c,
        (col(c) - lit(meanAndStd.getAs(s"${c}_avg")))
          /  lit(meanAndStd.getAs(s"${c}_std")))
    }


//    val a = cols.map(x =>
//      df.select(col(MetaFeatureDatasets.ColDs), col(ColAtt),
//      zScoreUDF( col(x), lit(meanAndStd.getAs(s"${x}_avg")),
//        lit(meanAndStd.getAs(s"${x}_std"))).as(x)))
//       .reduce(_.join(_, Seq(MetaFeatureDatasets.ColDs, ColAtt)))

    // scalastyle:off println
//    println(s"${zScoreDF.count()} :) ***")
//    zScoreDF.show(10)
    // scalastyle:on println
//    a
    zScoreDF

  }


  private def zScore(x: Double, avgVal: Double, stdVal: Double): Double = {
    (x - avgVal) / stdVal
  }

  private def getColumnsMeta(metaType: String): Seq[String] = metaType match {
    case "numeric" => NumericMeta.keySet.toSeq
    case "nominal" => NominalMeta.keySet.toSeq
    case "datasets" => MetaFeatureDatasets.cols()
    case _ => MetaFeatureDatasets.cols()
  }




}
