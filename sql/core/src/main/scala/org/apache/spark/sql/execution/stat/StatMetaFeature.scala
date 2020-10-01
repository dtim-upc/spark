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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Divide, Expression, GenericInternalRow, IsNull, Length}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.stat.metafeatures.{DataTypeMF, MetaFeature}
import org.apache.spark.sql.execution.stat.metafeatures.MetaFeaturesConf.{emptyMF, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String


object StatMetaFeature extends Logging{


  var Nrows: Double = 1.0

  def computeMetaFeature(ds: Dataset[_], overrideProfiling: Boolean = false): DataFrame = {


    val outputDs = ds.logicalPlan.output

    // TODO: Check what types are UDTs, arrays, structs, and maps to handle them
    val nominalA = outputDs.filter(a => a.dataType.isInstanceOf[StringType])
//    val numericA = outputDs.filter(a => a.dataType.isInstanceOf[NumericType])

    Nrows = ds.count.toDouble
    val numberAttributes = outputDs.size.toDouble

    // TODO: Handle when more files and save metadata and when is null!!!
    val filename = ds.inputFiles(0).split("/").last
    val path = ds.inputFiles(0)

    val nomPath = pathMF(path, filename, "nominal")
//    val numPath = pathMF(path, filename, "numeric")

    if(overrideProfiling) {
      if ( nominalA.size > 0 ) {

        val selectMetaFeatures = Nominal.filter(_.dependant != true).filter(_.isCustom != true)
        val selectMetaFeaturesDep = Nominal.filter(_.dependant == true)
        val selectMetaFeaturesCustom = Nominal.filter(_.isCustom == true)
        val selectMetaFeaturesNomFeq = selectMetaFeatures.filter(_.useFreqCnt == true)
          .filter(_.isQuartile != true)
        val selectMetaFeaturesSoundex = selectMetaFeatures.filter(_.useSoundexCnt == true)
          .filter(_.isQuartile != true)
        val selectMetaFeaturesNom = selectMetaFeatures.filter(_.useFreqCnt !=true)
          .filter(_.useWordCnt != true).filter(_.useSoundexCnt != true)
        val selectMetaFeaturesWords = selectMetaFeatures.filter(_.useWordCnt == true)

        if (selectMetaFeaturesNomFeq.size > 0) {


          val computedMetaFeatures = compute(selectMetaFeaturesNom, nominalA, ds)
          val nominalDs2 = Dataset.ofRows(ds.sparkSession, createMFRel(
            selectMetaFeaturesNom, nominalA, filename, computedMetaFeatures))

          val tmpDF = nominalA.map(att => compute(selectMetaFeaturesNomFeq, Seq(att), ds, "freq"))
          val selectMetaFeaturesQuartiles = selectMetaFeatures.filter(_.isQuartile == true)

          val nominalDs = tmpDF.zipWithIndex.map(tuple => {

            var df = ofRows(ds.sparkSession, createMFRel(
              selectMetaFeaturesNomFeq, Seq(nominalA(tuple._2)), filename, tuple._1))

            if (selectMetaFeaturesQuartiles.size > 0) {
              df = computeQuartiles(df, ds, selectMetaFeaturesQuartiles, Seq(nominalA(tuple._2)))
            }

            //              if (selectMetaFeatures.filter(_.key == Entropy.key).size > 0) {
            //                df = computeEntropy(df, ds, nominalA(tuple._2))
            //              }
            if (selectMetaFeaturesCustom.size > 0) {
              df = computeCustom(df, ds, nominalA(tuple._2), selectMetaFeaturesCustom, nominalDs2)
            }


            df

          }).reduce(_.union(_))



          val computedMetaFeatures3 = compute(selectMetaFeaturesWords, nominalA, ds, "wordCnt")
          val nominalDs3 = Dataset.ofRows(ds.sparkSession, createMFRel(
            selectMetaFeaturesWords, nominalA, filename, computedMetaFeatures3))



          val nominals = nominalDs.join(nominalDs2, Seq("ds_name", "att_name"))
            .join(nominalDs3, Seq("ds_name", "att_name"))
          //              .join(nominalDs4, Seq("ds_name", "att_name"))

          val nominalAtt = computeDependants(selectMetaFeaturesDep, nominals)

          saveMF(nomPath, nominalAtt)
        } else {
          val emptyDF = Dataset.ofRows(ds.sparkSession, createMFRel(
            Nominal, Seq(), filename, InternalRow.fromSeq(Seq())))
          saveMF(nomPath, emptyDF)
        }

      } else {
        val emptyDF = Dataset.ofRows(ds.sparkSession, createMFRel(
          Nominal, Seq(), filename, InternalRow.fromSeq(Seq())))
        saveMF(nomPath, emptyDF)
      }
      ds.sparkSession.read.load(nomPath)
    } else {
      try {
        val nominalAtt = ds.sparkSession.read.load(nomPath)
        //      val numericAtt = ds.sparkSession.read.load(numPath)
        // scalastyle:off println
        //      println("READ META")
        // scalastyle:on println
        nominalAtt
      } catch {
        case e:
          // scalastyle:off println
          AnalysisException =>
          // nominals
          if ( nominalA.size > 0 ) {

            val selectMetaFeatures = Nominal.filter(_.dependant != true).filter(_.isCustom != true)
            val selectMetaFeaturesDep = Nominal.filter(_.dependant == true)
            val selectMetaFeaturesCustom = Nominal.filter(_.isCustom == true)
            val selectMetaFeaturesNomFeq = selectMetaFeatures.filter(_.useFreqCnt == true)
              .filter(_.isQuartile != true)
            val selectMetaFeaturesSoundex = selectMetaFeatures.filter(_.useSoundexCnt == true)
              .filter(_.isQuartile != true)
            val selectMetaFeaturesNom = selectMetaFeatures.filter(_.useFreqCnt !=true)
              .filter(_.useWordCnt != true).filter(_.useSoundexCnt != true)
            val selectMetaFeaturesWords = selectMetaFeatures.filter(_.useWordCnt == true)

            if (selectMetaFeaturesNomFeq.size > 0) {


              val computedMetaFeatures = compute(selectMetaFeaturesNom, nominalA, ds)
              val nominalDs2 = Dataset.ofRows(ds.sparkSession, createMFRel(
                selectMetaFeaturesNom, nominalA, filename, computedMetaFeatures))

              val tmpDF = nominalA.map(att => compute(selectMetaFeaturesNomFeq, Seq(att), ds, "freq"))
              val selectMetaFeaturesQuartiles = selectMetaFeatures.filter(_.isQuartile == true)

              val nominalDs = tmpDF.zipWithIndex.map(tuple => {

                var df = ofRows(ds.sparkSession, createMFRel(
                  selectMetaFeaturesNomFeq, Seq(nominalA(tuple._2)), filename, tuple._1))

                if (selectMetaFeaturesQuartiles.size > 0) {
                  df = computeQuartiles(df, ds, selectMetaFeaturesQuartiles, Seq(nominalA(tuple._2)))
                }

                //              if (selectMetaFeatures.filter(_.key == Entropy.key).size > 0) {
                //                df = computeEntropy(df, ds, nominalA(tuple._2))
                //              }
                if (selectMetaFeaturesCustom.size > 0) {
                  df = computeCustom(df, ds, nominalA(tuple._2), selectMetaFeaturesCustom, nominalDs2)
                }


                df

              }).reduce(_.union(_))



              val computedMetaFeatures3 = compute(selectMetaFeaturesWords, nominalA, ds, "wordCnt")
              val nominalDs3 = Dataset.ofRows(ds.sparkSession, createMFRel(
                selectMetaFeaturesWords, nominalA, filename, computedMetaFeatures3))

              //            val nominalDs4 = nominalA
              //              .map(att => compute(selectMetaFeaturesSoundex, Seq(att), ds, "soundex"))
              //              .zipWithIndex.map(tuple => {
              //                var df = ofRows(ds.sparkSession, createMFRel(
              //                  selectMetaFeaturesSoundex, Seq(nominalA(tuple._2)), filename, tuple._1))
              //                df
              //              }).reduce(_.union(_))
              //            val computedMetaFeatures4 = compute(selectMetaFeaturesSoundex,
              //              nominalA, ds, "soundex")
              //            val nominalDs4 = Dataset.ofRows(ds.sparkSession, createMFRel(
              //              selectMetaFeaturesSoundex, nominalA, filename, computedMetaFeatures4))

              val nominals = nominalDs.join(nominalDs2, Seq("ds_name", "att_name"))
                .join(nominalDs3, Seq("ds_name", "att_name"))
              //              .join(nominalDs4, Seq("ds_name", "att_name"))

              val nominalAtt = computeDependants(selectMetaFeaturesDep, nominals)

              saveMF(nomPath, nominalAtt)
            } else {
              val emptyDF = Dataset.ofRows(ds.sparkSession, createMFRel(
                Nominal, Seq(), filename, InternalRow.fromSeq(Seq())))
              saveMF(nomPath, emptyDF)
            }

          } else {
            val emptyDF = Dataset.ofRows(ds.sparkSession, createMFRel(
              Nominal, Seq(), filename, InternalRow.fromSeq(Seq())))
            saveMF(nomPath, emptyDF)
          }
          ds.sparkSession.read.load(nomPath)
        case _:
          // TODO: handle it@
          // scalastyle:off println
          Throwable => println("Got some other kind of Throwable exception")
          // scalastyle:on println
          createDF(ds, Seq(("1"->1)))
      }
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


  def compute(metaFeatures: Seq[MetaFeature], selectedCols: Seq[Attribute], ds: Dataset[_],
              dType: String = ""): InternalRow = {

    val SeqExpressions = metaFeatures.map{meta => meta.key match {
      case Cardinality.key => (child: Expression) => Count(child).toAggregateExpression(true)
      case MeanMF.key => (child: Expression) => Average(child).toAggregateExpression()
      case StdMF.key => (child: Expression) => StddevPop(child).toAggregateExpression()
      case MinMF.key => (child: Expression) => Min(child).toAggregateExpression()
      case MaxMF.key => (child: Expression) => Max(child).toAggregateExpression()
//      case Incompleteness.key => (child: Expression) => Divide(Multiply(Sum(Cast(IsNull(child),
//        IntegerType)).toAggregateExpression(), lit(100).expr), lit(Nrows).expr)
      case Incompleteness.key => (child: Expression) => Divide(Sum(Cast(IsNull(child),
        IntegerType)).toAggregateExpression(), lit(Nrows).expr)

      case FrequencyAVG.key => (child: Expression) => Average(child).toAggregateExpression()
      case FrequencyMin.key => (child: Expression) => Min(child).toAggregateExpression()
      case FrequencyMax.key => (child: Expression) => Max(child).toAggregateExpression()
      case FrequencySD.key => (child: Expression) => StddevPop(child).toAggregateExpression()
      case LongestWord.key => (child: Expression) => Length(Max(child).toAggregateExpression())
      case ShortestWord.key => (child: Expression) => Length(Min(child).toAggregateExpression())
      case AverageWord.key => (child: Expression) => Average(Length(child))
        .toAggregateExpression()

//      case SoundexAVG.key => (child: Expression) => Average(child).toAggregateExpression()
//      case SoundexMin.key => (child: Expression) => Min(child).toAggregateExpression()
//      case SoundexMax.key => (child: Expression) => Max(child).toAggregateExpression()
//      case SoundexSD.key => (child: Expression) => StddevPop(child).toAggregateExpression()

      case First_word.key => (child: Expression) => Min(child).toAggregateExpression()
      case Last_word.key => (child: Expression) => Max(child).toAggregateExpression()

      case numberWords.key => (child: Expression) => Sum(child).toAggregateExpression()
      case AvgWords.key => (child: Expression) => Average(child).toAggregateExpression()
      case MinWords.key => (child: Expression) => Min(child).toAggregateExpression()
      case MaxWords.key => (child: Expression) => Max(child).toAggregateExpression()
      case SdWords.key => (child: Expression) => StddevPop(child).toAggregateExpression()

      case PctStdMF.key => (child: Expression) =>
        StddevPop(Divide(child, lit(Nrows).expr)).toAggregateExpression()

      case _ => throw new IllegalArgumentException(s"$meta is not a recognised metafeature")
    }
    }

    // TODO: change to get first word _val
    val aggExprs = SeqExpressions.zipWithIndex.flatMap { case (func, i) =>

      if (metaFeatures(i).useRawVal) {

        selectedCols.map(c => Column(Cast(func(col(s"${c.name}_raw").expr), StringType)).as(c.name))
      } else {
        selectedCols.map(c => Column(Cast(func(col(c.name).expr), StringType)).as(c.name))
      }



    }

//    val aggExprs = SeqExpressions.flatMap { func =>
//      selectedCols.map(c => Column(Cast(func(Column(c.name).expr), StringType)).as(c.name))
//    }

    dType match {
      case "wordCnt" =>
//        selectedCols.map(colN =>
//          ds.select(colN.name).na.drop
//        ).reduce(_.union(_))
//
        ds.select(selectedCols.map(x => size(split(trim(col(x.name)), " ")).as(x.name)): _*)
        .select(aggExprs: _*).queryExecution.toRdd.collect().head
      case "soundex" =>
        getFrequenciesCount(ds, selectedCols(0).name, sdx = true)
          .select(aggExprs: _*).queryExecution.toRdd.collect().head
      case "freq" => getFrequenciesCount(ds, selectedCols(0).name)
        .select(aggExprs: _*).queryExecution.toRdd.collect().head
      case "" => ds.select(selectedCols.map(att => trim(col(att.name)).as(att.name)): _*)
        .select(aggExprs: _*).queryExecution.toRdd.collect().head
//      case _ => ds.select(aggExprs: _*).queryExecution.toRdd.collect().head
    }


  }



  def computeCustom(metaDF: DataFrame, ds: Dataset[_], colName: Attribute,
                    mf: Seq[MetaFeature], dfMeta: Dataset[_])
  : DataFrame = {

    var df: DataFrame = metaDF

    mf.map(meta => meta.key match {

      case TypeInformation.key =>
        // scalastyle:off
//        println(s"profiling patterns ${colName.name}")
        // scalastyle:on println

        val extractType = udf(extractTypeInfo(_: String): DataTypeMF)


        val cardinality = dfMeta.filter(col("att_name") === colName.name)
          .select(col(Cardinality.name).cast(DoubleType))
          .collect()(0).getAs[Double](Cardinality.name)



//        var tmp = ds.filter(col(colName.name) =!= " ")
//          .select(colName.name).na.drop.distinct
//          .withColumn(TypeInformation.name, extractType(Column(colName.name)))
//          .select(s"${TypeInformation.name}.*")

        var tmp = ds.filter(col(colName.name) =!= " ")
          .select(substring(col(colName.name), 0, 25).as(colName.name))
          .na.drop.distinct
          .withColumn(TypeInformation.name, extractType(Column(colName.name)))
          .select(s"${TypeInformation.name}.*")

        if (cardinality >= 1000) {

          val randomS = ds.select(substring(col(colName.name), 0, 20).as(colName.name))
            .na.drop.distinct.randomSplit(Array(0.1, 0.9))

          var tmpDataFrame = randomS(0).limit(1000)

          if(tmpDataFrame.count() <= 10){
            tmpDataFrame = ds.select(substring(col(colName.name), 0, 20).as(colName.name))
              .na.drop.distinct.limit(1000)
          }



//          var tmpDataFrame = ds
//            .select(substring(col(colName.name), 0, 20).as(colName.name))
//            .na.drop.distinct
//            .sample(false, 0.1)

//          if(cardinality > 100000){
//            tmpDataFrame = ds
//              .select(substring(col(colName.name), 0, 20).as(colName.name))
//              .na.drop.distinct
//              .sample(false, 0.1).limit(10000)
//          }

          // scalastyle:off
//          println(s"profiling patterns sample. Before ${cardinality}, Now ${tmpDataFrame.count}")
          // scalastyle:on println

//          if( tmpDataFrame.count == 0) {
//            tmpDataFrame = ds
//              .select(substring(col(colName.name), 0, 20).as(colName.name))
//              .na.drop.distinct.limit(1000)
//            println(s"profiling patterns sample. Before ${cardinality}, Now ${tmpDataFrame.count}")
//          }

           tmp = tmpDataFrame
            .withColumn(TypeInformation.name, extractType(Column(colName.name)))
            .select(s"${TypeInformation.name}.*")

        }



        val attTypeDF = tmp.select("dataType").groupBy("dataType").agg( expr(s"count(*) as cnt"))
          .orderBy(desc("cnt"))

        if (attTypeDF.limit(1).collect().length > 0) {
          df = df.withColumn("dataType",
            lit(attTypeDF.limit(1).collect()(0).getAs[String]("dataType")))
        } else {
          df = df.withColumn("dataType",
            lit("IsNull"))
        }



        val attMap = attTypeDF.collect
          .map(r => {
            r.getAs[String]("dataType") -> r.getAs[Long]("cnt")
          }).toMap

        var sizeDistinct = attMap.foldLeft(0.0)( _ + _._2)
        sizeDistinct = if (sizeDistinct == 0) 1 else sizeDistinct
//        attTypeDF.show

        // scalastyle:off
//        println(s"Size distinct: ${sizeDistinct}")
        // scalastyle:on println

        typesList.map { case (key, colname) =>
          df = df.withColumn(colname,
            lit(attMap.getOrElse(key, 0).toString.toDouble / sizeDistinct))
          df
        }

        val attSpecificTypeDF = tmp.select("specificType").groupBy("specificType")
          .agg( expr(s"count(*) as cnt")).orderBy(desc("cnt"))

        if (attSpecificTypeDF.limit(1).collect().length > 0) {
          df = df.withColumn("specificType",
            lit(attSpecificTypeDF.limit(1).collect()(0).getAs[String]("specificType")))
        } else {
          df = df.withColumn("specificType",
            lit("isNull"))
        }


        val attMapSpecificType = attSpecificTypeDF.collect.map(r => {

          r.getAs[String]("specificType") -> r.getAs[Long]("cnt")
        }).toMap
        sizeDistinct = attMapSpecificType.foldLeft(0.0)(_ + _._2)
        sizeDistinct = if (sizeDistinct == 0) 1 else sizeDistinct

//        attSpecificTypeDF.show
        specifyTypesList.map{case (key, colname) =>
          df = df.withColumn(colname,
            lit(attMapSpecificType.getOrElse(key, 0).toString.toDouble / sizeDistinct))
          df
        }
      df


      case Entropy.key =>
        val e = getFrequenciesCount(ds, colName.name, p = true).select(
          (-sum(when(col(colName.name) === 0, lit(0)).otherwise(
            col(colName.name) * log2(col(colName.name))))).as(colName.name)).take(1).head.get(0)

        df = df.withColumn(Entropy.name, lit(e))
      case Freq_word.key =>
        val renamedCol = s"${colName.name}_raw"

        val topk = ds.select(lower(trim(col(colName.name))).as(renamedCol)).na.drop
//          .groupBy(renamedCol).agg( expr(s"count(*) as ${colName.name}").as(colName.name))
          .filter(col(renamedCol) =!= " ")
          .groupBy(renamedCol).agg( expr(s"count(*)").as(colName.name))
          .orderBy(desc(colName.name)).limit(KWords).collect()
          .map(r => r.getAs[String](renamedCol)).toList

        df = df.withColumn(Freq_word.name, typedLit(topk))
        df = df.withColumn(Freq_wordClean.name, typedLit(topk))

        df
      case Freq_wordSoundex.key =>
        val renamedCol = s"${colName.name}_raw"

        val topk = ds.select(soundex(trim(col(colName.name))).as(renamedCol)).na.drop
          .filter(col(renamedCol) =!= " ")
          .groupBy(renamedCol).agg( expr(s"count(*)").as(colName.name))
          .orderBy(desc(colName.name)).limit(KWords).collect()
          .map(r => r.getAs[String](renamedCol)).toList

        df = df.withColumn(Freq_wordSoundex.name, typedLit(topk))
        df
      case _ =>
        df
    })

    df

  }


  def extractTypeInfo(s: String): DataTypeMF = s match {
    case "" =>
      DataTypeMF(NonAlphanumericStr, "otherST")
    case " " =>
      DataTypeMF(AlphanumericStr, spacesStr)
    case DateRGX(_*) =>
      DataTypeMF(DateTimeStr, "date")
    case NumericRGX(_*) =>
      DataTypeMF(NumericStr, "otherST")
    case EmailRGX(_*) =>
      DataTypeMF(AlphanumericStr, emailStr)
    case IPRGX(_*) =>
      DataTypeMF(AlphanumericStr, ipStr)
    case PhoneRGX(_*) =>
      DataTypeMF(AlphanumericStr, phoneStr)
    case TimeRGX(_*) =>
      DataTypeMF(DateTimeStr, "time")
    case DateTimeRGX(_*) =>
      DataTypeMF(DateTimeStr, "datetime")
    case URLRGX(_*) =>
      DataTypeMF(AlphanumericStr, urlStr)
    case AlphabeticRGX(_*) =>
      DataTypeMF(AlphabeticStr, "otherST")
    case UsernameRGX(_*) =>
      DataTypeMF(AlphanumericStr, usernameStr)
    case AlphanumericRGX(_*) =>
      DataTypeMF(AlphanumericStr, generalStr)
    case AlphanumericPhrasesRGX(_*) =>
      DataTypeMF(AlphanumericStr, phrasesStr)
    case NonAlphanumericRGX(_*) =>
      DataTypeMF(NonAlphanumericStr, "otherST")
    case _ =>
      DataTypeMF(AlphanumericStr, otherStr) // contains unknown symbols with alphanumeric
  }

//  def computeEntropy(metaDF: DataFrame, ds: Dataset[_], colname: Attribute): DataFrame = {
//    var df: DataFrame = metaDF
//
//
//      val e = getFrequenciesCount(ds, colname.name, true).select(
//        (-sum(when(col(colname.name) === 0, lit(0))
// .otherwise( col(colname.name) * log2(col(colname.name))))).as(colname.name)).take(1).head.get(0)
//
//      df = df.withColumn(Entropy.name, lit(e))
//
//
//    df
//
//  }

//  def computeEntropy(metaDF: DataFrame, ds: Dataset[_], cols: Seq[Attribute]): DataFrame = {
//    var df: DataFrame = metaDF
//
//    cols.map(_.name).map(colname => {
//      val e = getFrequenciesCount(ds, colname, true).select(
//        (-sum(when(col(colname) === 0, lit(0))
//          .otherwise( col(colname) * log2(col(colname))))).as(colname)).take(1).head.get(0)
//
//      df = df.withColumn(Entropy.name, lit(e))
//
//    })
//    df
//
//  }

  def computeQuartiles(metaDF: DataFrame, ds: Dataset[_], meta: Seq[MetaFeature],
                       cols: Seq[Attribute], sdx: Boolean = false): DataFrame = {


    var df: DataFrame = metaDF

    val quartiles = meta.filter(_.useSoundexCnt == false)
      .map(m => m.key match {
          // TODO: delete first, second and third since are for numeric
      case FirstQuartile.key => 0.25
      case SecondQuartile.key => 0.5
      case ThirdQuartile.key => 0.75
      case Frequency1Qo.key => 0.125
      case Frequency1Q.key => 0.25
      case Frequency3Qo.key => 0.375
      case Frequency2Q.key => 0.5
      case Frequency5Qo.key => 0.625
      case Frequency3Q.key => 0.75
      case Frequency7Qo.key => 0.875
    })

    cols.map(colname => {
      val q = getFrequenciesCount(ds, colname.name, true).stat
        .approxQuantile(colname.name, quartiles.toArray, 0.0)

      if(q.isEmpty) {
        // attribute has 100% of missing values
        meta.map(m => df = df.withColumn(m.name, lit(0)))
      } else {
       q.zipWithIndex.map(tuple => df = df.withColumn(meta(tuple._2).name, lit(tuple._1)))
      }

    })


//
//    if (meta.filter(_.useSoundexCnt == true).size > 0) {
//      cols.map(colname => {
//        val q = getFrequenciesCount(ds, colname.name, sdx = true).stat
//          .approxQuantile(colname.name, Array(0.25, 0.5, 0.75), 0.0)
//
//          if (q.length > 0) {
//            df = df.withColumn(Soundex1Q.name, lit(q(0)))
//            df = df.withColumn(Soundex2Q.name, lit(q(1)))
//            df = df.withColumn(Soundex3Q.name, lit(q(2)))
//          } else {
//            df = df.withColumn(Soundex1Q.name, lit(0))
//            df = df.withColumn(Soundex2Q.name, lit(0))
//            df = df.withColumn(Soundex3Q.name, lit(0))
//          }
//        df
//
//      })
//    }

    df
  }

  def getFrequenciesCount(ds: Dataset[_], nameCol: String, p: Boolean = false,
                          sdx: Boolean = false): Dataset[_] = {


    val renameCol = s"${nameCol}_raw"
    var df = ds.filter(col(nameCol) =!= " ").select(trim(col(nameCol)).as(renameCol))
      .na.drop.groupBy(renameCol)
      .agg( expr(s"count(*)").as(nameCol))

    if (sdx) { // soundex
      df = ds.filter(col(nameCol) =!= " ").select(soundex(trim(col(nameCol))).as(renameCol))
        .na.drop.groupBy(renameCol)
        .agg( expr(s"count(*)").as(nameCol))
    }

//    var df = ds.select(nameCol).na.drop.groupBy(nameCol).agg( expr("count(*) as cnt"))
//      .withColumn(nameCol, col("cnt")).drop("cnt")

    if(p) { // percentage
      df = df.select(col(nameCol).divide(Nrows).as(nameCol))
    }
    df
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
        df = df.withColumn(PctMinMF.name, col(FrequencyMin.name) / Nrows)
      case PctMaxMF.key =>
        df = df.withColumn(PctMaxMF.name, col(FrequencyMax.name) / Nrows)

      case Uniqueness.key =>
        df = df.withColumn(Uniqueness.name, col(Cardinality.name)/ Nrows)
      case BestContainment.key =>
        df = df.withColumn(BestContainment.name, col(Cardinality.name))
      case Constancy.key =>
        df = df.withColumn(Constancy.name, col(FrequencyMax.name)/ Nrows)

      case FrequencyIQR.key =>
        df = df.withColumn(FrequencyIQR.name, col(Frequency3Q.name) - col(Frequency1Q.name) )

//      case SoundexIQR.key =>
//        df = df.withColumn(SoundexIQR.name, col(Soundex3Q.name) - col(Soundex1Q.name) )
      case emptyMF.key =>
//        df = df.withColumn(emptyMF.name, when(col(Incompleteness.name) === 1, 1).otherwise(0))
        df = df.withColumn(emptyMF.name, when(col(Incompleteness.name) === 1, 1).otherwise(0))


      case isBinary.key =>
        df = df.withColumn(isBinary.name, when(col(Cardinality.name) === 2, 1).otherwise(0))

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


    /**
     *
     * @param df need to have colum ds_name and att_name
     * @param metaType
     * @return
     */
    def normalizeDF(df: Dataset[_], metaType: String): DataFrame = {
  //    val zScoreUDF = udf(zScore(_: Double, _: Double, _: Double): Double)
      val cols = getMetaFeatures(metaType).filter(_.normalize).filter(_.normalizeType == 0)

      val aggExprsAvg = Seq((child: Expression) => Average(child).toAggregateExpression())
        .flatMap { func => cols.map(c => Column(Cast(func(Column(c.name).expr), StringType))
          .as(s"${c.name}_avg"))
      }
      val aggExprsSD = Seq((child: Expression) => StddevPop(child).toAggregateExpression())
        .flatMap { func => cols.map(c => Column(Cast(func(Column(c.name).expr), StringType))
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
      val dataF = zScoreDF.toDF()
      df.sparkSession.createDataFrame(dataF.rdd, dataF.schema).cache()

    }

    def getMetaFeatures(metaType: String): List[MetaFeature] = metaType match {
      case "numeric" => Numeric
      case _ => Nominal
    }



}
