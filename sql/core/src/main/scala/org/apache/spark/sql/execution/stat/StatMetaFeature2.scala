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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Divide, Expression, GenericInternalRow, IsNull, Length, Multiply}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, StddevPop, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeature
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf._
import org.apache.spark.sql.functions.{col, expr, lit, when}
import org.apache.spark.sql.types.{IntegerType, NumericType, StringType}
import org.apache.spark.unsafe.types.UTF8String


object StatMetaFeature2 extends Logging{


  var Nrows: Double = 1.0

  def computeMetaFeature(ds: Dataset[_]): (DataFrame, DataFrame) = {


    val outputDs = ds.logicalPlan.output

    // TODO: Check what types are UDTs, arrays, structs, and maps to handle them
    val nominalA = outputDs.filter(a => a.dataType.isInstanceOf[StringType])
    val numericA = outputDs.filter(a => a.dataType.isInstanceOf[NumericType])

    Nrows = ds.count.toDouble
    val numberAttributes = outputDs.size.toDouble

    // TODO: Handle when more files and save metadata and when is null!!!
    val filename = ds.inputFiles(0).split("/").last
    val path = ds.inputFiles(0)

    val nomPath = pathMF(path, filename, "nominal")
    val numPath = pathMF(path, filename, "numeric")

    try {
      val nominalAtt = ds.sparkSession.read.load(nomPath)
      val numericAtt = ds.sparkSession.read.load(numPath)
      // scalastyle:off println
       println("READ META")
        // scalastyle:on println
      ( numericAtt, nominalAtt)
    } catch {
      case e:
        // scalastyle:off println
        AnalysisException => println("Couldn't find that file.")
        // scalastyle:on println
        // compute content meta-features

        if( numericA.size > 0) {

          val selectMetaFeatures = Numeric.filter(_.dependant != true)
          val selectMetaFeaturesDep = Numeric.filter(_.dependant == true)
          val computedMetaFeatures = compute(selectMetaFeatures, numericA, ds)


          val numericDS = Dataset.ofRows(ds.sparkSession, createMFRel(
            selectMetaFeatures, numericA, filename, computedMetaFeatures))

          val numericAtt = computeDependants(selectMetaFeaturesDep, numericDS)
            .withColumn("isEmpty", when(col(Incompleteness.name) === 100, 1).otherwise(0))
          saveMF(numPath, numericAtt)
        }



        // nominals
        if ( nominalA.size > 0 ) {

          val selectMetaFeatures = Nominal.filter(_.dependant != true)
          val selectMetaFeaturesDep = Nominal.filter(_.dependant == true)
          val selectMetaFeaturesNomFeq = selectMetaFeatures.filter(_.useFreqCnt == true)
            .filter(_.isQuartile != true)
          val selectMetaFeaturesNom = selectMetaFeatures.filter(_.useFreqCnt !=true)

          if (selectMetaFeaturesNomFeq.size > 0) {


            val tmpDF = nominalA.map(att => compute(selectMetaFeaturesNomFeq, Seq(att), ds, true) )
            val selectMetaFeaturesQuartiles = selectMetaFeatures.filter(_.isQuartile == true)

            val nominalDs = tmpDF.zipWithIndex.map(tuple => {

              val df = ofRows(ds.sparkSession, createMFRel(
                selectMetaFeaturesNomFeq, Seq(nominalA(tuple._2)), filename, tuple._1))

              if (selectMetaFeaturesQuartiles.size > 0) {
                computeQuartiles(df, ds, selectMetaFeaturesQuartiles, Seq(nominalA(tuple._2)))
              }

              df

            }).reduce(_.union(_))

            val computedMetaFeatures = compute(selectMetaFeaturesNom, nominalA, ds)
            val nominalDs2 = Dataset.ofRows(ds.sparkSession, createMFRel(
              selectMetaFeaturesNom, nominalA, filename, computedMetaFeatures))


            val nominals = nominalDs.join(nominalDs2, Seq("ds_name", "att_name"))

            val nominalAtt = computeDependants(selectMetaFeaturesDep, nominals)
              .withColumn("isEmpty", when(col(Incompleteness.name) === 100, 1).otherwise(0))

            saveMF(nomPath, nominalAtt)
          }

        }
        (ds.sparkSession.read.load(numPath), ds.sparkSession.read.load(nomPath))
      case _:
        // TODO: handle it@
        // scalastyle:off println
        Throwable => println("Got some other kind of Throwable exception")
        // scalastyle:on println
        (createDF(ds, Seq(("1"->1))), createDF(ds, Seq(("1"->1))))
    }

  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def pathMF(path: String, fn1: String, meta: String): String = {
    val fn = fn1.replace(".", "")
    path.replace(fn1, s".${fn}/.${meta}")
  }

  def saveMF(path: String, ds: Dataset[_]): Unit = {
    ds.write.mode(SaveMode.Overwrite).format("parquet").save(path)
  }


  private def getColumns(k: String, num: Seq[String], nom: Seq[String], all: Seq[String])
    : Seq[String] = k match {
    case "numeric" => num
    case "nominal" => nom
    case "all" => all
  }



  def createDF(ds: Dataset[_], data: Seq[(String, Long)]): DataFrame = {
    val res = Array.fill[InternalRow](1) {new GenericInternalRow(data.length)}
    for ((x, i) <- data.view.zipWithIndex) res(0).update(i, UTF8String.fromString(x._2 + ""))
    val output = data.map(a => AttributeReference(a._1.toString, StringType)())
    Dataset.ofRows(ds.sparkSession, LocalRelation(output, res))
  }


  def compute(metaFeatures: Seq[MetaFeature], selectedCols: Seq[Attribute], ds: Dataset[_]):
  InternalRow = {
    compute(metaFeatures, selectedCols, ds, false)
  }

  def compute(metaFeatures: Seq[MetaFeature], selectedCols: Seq[Attribute], ds: Dataset[_],
              freqCount: Boolean): InternalRow = {

    val SeqExpressions = metaFeatures.map{meta => meta.key match {
        case Cardinality.key => (child: Expression) => Count(child).toAggregateExpression(true)
        case MeanMF.key => (child: Expression) => Average(child).toAggregateExpression()
        case StdMF.key => (child: Expression) => StddevPop(child).toAggregateExpression()
        case MinMF.key => (child: Expression) => Min(child).toAggregateExpression()
        case MaxMF.key => (child: Expression) => Max(child).toAggregateExpression()
        case Incompleteness.key => (child: Expression) => Divide(Multiply(Sum(Cast(IsNull(child),
          IntegerType)).toAggregateExpression(), lit(100).expr), lit(Nrows).expr)

        case FrequencyAVG.key => (child: Expression) => Average(child).toAggregateExpression()
        case FrequencyMin.key => (child: Expression) => Min(child).toAggregateExpression()
        case FrequencyMax.key => (child: Expression) => Max(child).toAggregateExpression()
        case FrequencySD.key => (child: Expression) => StddevPop(child).toAggregateExpression()
        case LongestWord.key => (child: Expression) => Length(Max(child).toAggregateExpression())
        case ShortestWord.key => (child: Expression) => Length(Min(child).toAggregateExpression())
        case AverageWord.key => (child: Expression) => Average(Length(child))
          .toAggregateExpression()


        case PctStdMF.key => (child: Expression) =>
          StddevPop(Divide(Multiply(child, lit(100).expr), lit(Nrows).expr)).toAggregateExpression()

        case _ => throw new IllegalArgumentException(s"$meta is not a recognised metafeature")
      }
    }

    val aggExprs = SeqExpressions.flatMap { func =>
      selectedCols.map(c => new Column(Cast(func(Column(c.name).expr), StringType)).as(c.name))
    }

    freqCount match {
      case true => getFrequenciesCount(ds, selectedCols(0).name)
        .select(aggExprs: _*).queryExecution.toRdd.collect().head
      case _ => ds.select(aggExprs: _*).queryExecution.toRdd.collect().head
    }


  }

  def computeQuartiles(metaDF: DataFrame, ds: Dataset[_], meta: Seq[MetaFeature],
                       cols: Seq[Attribute]): DataFrame = {

    val m = ""

    var df: DataFrame = metaDF

    val quartiles = meta.map(m => m.key match {
      case FirstQuartile.key => 0.25
      case SecondQuartile.key => 0.5
      case ThirdQuartile.key => 0.75
      case Frequency1Q.key => 0.25
      case Frequency2Q.key => 0.5
      case Frequency3Q.key => 0.75
    })

    cols.map(colname => {
      getFrequenciesCount(ds, colname.name).stat
        .approxQuantile(colname.name, quartiles.toArray, 0.0)
        .zipWithIndex.map(tuple => df = df.withColumn(meta(tuple._2).name, lit(tuple._1)))
    })
    df
  }

  def getFrequenciesCount(ds: Dataset[_], nameCol: String): Dataset[_] = {
    ds.select(nameCol).na.drop.groupBy(nameCol)
      .agg( expr("count(*) as cnt")).withColumn(nameCol, col("cnt"))

  }

//  def computeTest(metaFeatures: Seq[String], selectedCols: Seq[Attribute], ds: Dataset[_]):
//  DataFrame = {
//
//    val SeqExpressions = metaFeatures.map{meta => meta match {
//      case Cardinality.key => (child: Expression) => Count(child).toAggregateExpression(true)
//      case MeanMF.key => (child: Expression) => Average(child).toAggregateExpression()
//      case StdMF.key => (child: Expression) => StddevPop(child).toAggregateExpression()
//      case MinMF.key => (child: Expression) => Min(child).toAggregateExpression()
//      case MaxMF.key => (child: Expression) => Max(child).toAggregateExpression()
//      case Incompleteness.key => (child: Expression) =>
//        Sum( Cast(IsNull(child), IntegerType)).toAggregateExpression()
//      case _ => throw new IllegalArgumentException(s"$meta is not a recognised metafeature")
//    }
//    }
//
//    val aggExprs = SeqExpressions.flatMap { func =>
//      selectedCols.map(c => new Column(Cast(func(c), StringType)).as(c.name))
//
//    }
//
//    val aggResult = ds.select(aggExprs: _*)
//    aggResult
//  }


  def computeDependants(metaFeatures: Seq[MetaFeature], df1: DataFrame)
    : DataFrame = {

    var df: DataFrame = df1
    metaFeatures.map( mf => mf.key match {
      case RangeMF.key =>
        df = df.withColumn(RangeMF.name, col(MaxMF.name) - col(MinMF.name) )
      case CoVarMF.key =>
        df = df.withColumn(CoVarMF.name, col(StdMF.name) / col(MeanMF.name))

      case PctMinMF.key =>
        df = df.withColumn(PctMinMF.name, col(FrequencyMin.name) * 100 / Nrows)
      case PctMaxMF.key =>
        df = df.withColumn(PctMaxMF.name, col(FrequencyMax.name) * 100 / Nrows)

      case Uniqueness.key =>
        df = df.withColumn(Uniqueness.name, col(Cardinality.name)/ Nrows)
      case Constancy.key =>
        df = df.withColumn(Constancy.name, col(FrequencyMax.name)/ Nrows)


      case _ => throw new IllegalArgumentException(s"${mf} is not a recognised metafeature")
    })
    df
  }




  def createMFRel(meta: List[MetaFeature], attSeq: Seq[Attribute], f: String, aggResult: InternalRow
                 ): LocalRelation = {
    val rowsSize = attSeq.length
    val result = Array.fill[InternalRow](rowsSize)
      {new GenericInternalRow(meta.size + 2)} // for attName and ds_name

    var rowIndex = 0
    val columnsOutput = meta.map(_.name)

    for ((att, index) <- attSeq.zipWithIndex) {
      // compute content-level metadata
      result(rowIndex).update(0, UTF8String.fromString(f))// filename
      result(rowIndex).update(1, UTF8String.fromString(att.name))// att name
      var colIndex = 2
      for ((mf, indexMf) <- meta.zipWithIndex) {

//        val statVal = aggResult.getUTF8String(rowIndex * indexMf + index)
        val statVal = aggResult.getUTF8String(rowIndex + attSeq.size * indexMf)
        result(rowIndex).update(colIndex, statVal)
        colIndex += 1
      }
      rowIndex += 1
    }

    val output = AttributeReference("ds_name", StringType)() +:
      AttributeReference("att_name", StringType)() +:
      columnsOutput.map(a => AttributeReference(a, StringType)())
    LocalRelation(output, result)
  }



}
