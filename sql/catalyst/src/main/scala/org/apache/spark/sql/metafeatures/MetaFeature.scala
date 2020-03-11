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

package org.apache.spark.sql.metafeatures

import org.apache.spark.annotation.InterfaceStability

@InterfaceStability.Stable
case class MetaFeature(
    attributes: Array[MetaFeatureAttributes],
    dataset: MetaFeatureDataset
 ) {


  /** No-arg constructor for kryo. */
//  protected def this() = this(null, null, null, null, null, null)

  // override the default toString to be compatible with legacy parquet files.
//  override def toString: String = s"MetaFeatureAttributes($mean,$std,$min_val)"
}

class MetaFeatureDataset {
 private var _numberInstances: Long = _
 private var _numberAttributes: Int = _
 private var _dimensionality: Double = _
 private var _numberAttNominal: Int = _
 private var _numberAttNumeric: Int = _
 private var _percAttNominal: Long = _
 private var _percAttNumeric: Long = _
 private var _avgNominal: Double = _
 private var _stdNominal: Double = _
 private var _minNominal: Double = _
 private var _maxNominal: Double = _
 private var _avgNumeric: Double = _
 private var _stdNumeric: Double = _
 private var _minNumeric: Double = _
 private var _maxNumeric: Double = _
 private var _missingAttCnt: Double = _
 private var _missingAttPerc: Double = _
 private var _minMissing: Double = _
 private var _maxMissing: Double = _
 private var _minMissingPerc: Double = _
 private var _maxMissingPerc: Double = _
 private var _meanMissing: Double = _
 private var _meanMissingPerc: Double = _

 def numberInstances: Long = _numberInstances
 def numberAttributes: Int = _numberAttributes
 def dimensionality: Double = _dimensionality
 def numberAttNominal: Int = _numberAttNominal
 def numberAttNumeric: Int = _numberAttNumeric
 def percAttNominal: Long = _percAttNominal
 def percAttNumeric: Long = _percAttNumeric
 def avgNominal: Double = _avgNominal
 def stdNominal: Double = _stdNominal
 def minNominal: Double = _minNominal
 def maxNominal: Double = _maxNominal
 def avgNumeric: Double = _avgNumeric
 def stdNumeric: Double = _stdNumeric
 def minNumeric: Double = _minNumeric
 def maxNumeric: Double = _maxNumeric
 def missingAttCnt: Double = _missingAttCnt
 def missingAttPerc: Double = _missingAttPerc
 def minMissing: Double = _minMissing
 def maxMissing: Double = _maxMissing
 def minMissingPerc: Double = _minMissingPerc
 def maxMissingPerc: Double = _maxMissingPerc
 def meanMissing: Double = _meanMissing
 def meanMissingPerc: Double = _meanMissingPerc

 def numberInstances_= (newVal: Long): Unit = _numberInstances = newVal
 def numberAttributes_= (newVal: Int): Unit = _numberAttributes = newVal
 def dimensionality_= (newVal: Double): Unit = _dimensionality = newVal
 def numberAttNominal_= (newVal: Int): Unit = _numberAttNominal = newVal
 def numberAttNumeric_= (newVal: Int): Unit = _numberAttNumeric = newVal
 def percAttNominal_= (newVal: Long): Unit = _percAttNominal = newVal
 def percAttNumeric_= (newVal: Long): Unit = _percAttNumeric = newVal
 def avgNominal_= (newVal: Double): Unit = _avgNominal = newVal
 def stdNominal_= (newVal: Double): Unit = _stdNominal = newVal
 def minNominal_= (newVal: Double): Unit = _minNominal = newVal
 def maxNominal_= (newVal: Double): Unit = _maxNominal = newVal
 def avgNumeric_= (newVal: Double): Unit = _avgNumeric = newVal
 def stdNumeric_= (newVal: Double): Unit = _stdNumeric = newVal
 def minNumeric_= (newVal: Double): Unit = _minNumeric = newVal
 def maxNumeric_= (newVal: Double): Unit = _maxNumeric = newVal
 def missingAttCnt_= (newVal: Double): Unit = _missingAttCnt = newVal
 def missingAttPerc_= (newVal: Double): Unit = _missingAttPerc = newVal
 def minMissing_= (newVal: Double): Unit = _minMissing = newVal
 def maxMissing_= (newVal: Double): Unit = _maxMissing = newVal
 def minMissingPerc_= (newVal: Double): Unit = _minMissingPerc = newVal
 def maxMissingPerc_= (newVal: Double): Unit = _maxMissingPerc = newVal
 def meanMissing_= (newVal: Double): Unit = _meanMissing = newVal
 def meanMissingPerc_= (newVal: Double): Unit = _meanMissingPerc = newVal


 override def toString() : String = {
  "Number of instances: " + numberInstances + "\n" +
  "Number of attributes: " + numberAttributes
  "Dimensionality: " + dimensionality + "\n" +
  "Number of nominal attributes: " + numberAttNominal + "\n" +
  "Number of numeric attributes: " + numberAttNumeric + "\n" +
  "Percentage of nominal attributes: " + percAttNominal + "\n" +
  "Percentage of numeric attributes: " + percAttNumeric + "\n" +
  "Average of nominal values: " + avgNominal + "\n" +
  "Std of nominal values: " + stdNominal + "\n" +
  "Min number of nominal values: " + minNominal + "\n" +
  "Max number of nominal values: " + maxNominal + "\n" +
  "Average of numeric values: " + avgNumeric + "\n" +
  "Std of numeric values: " + stdNumeric + "\n" +
  "Min of numeric values: " + minNumeric + "\n" +
  "Max of numeric values: " + maxNumeric + "\n" +
  "Missing attribute count: " + missingAttCnt + "\n" +
  "Missing attribute percentage: " + missingAttPerc + "\n" +
  "Min number of missing values: " + minMissing + "\n" +
  "Max number of missing values: " + maxMissing + "\n" +
  "Min percentage of missing values: " + minMissingPerc + "\n" +
  "Max percentage of missing values: " + maxMissingPerc + "\n" +
  "Mean number of missing values: " + meanMissing + "\n" +
  "Mean percentage of missing values: " + meanMissingPerc + "\n"
 }
}

case class MetaFeatureAttributes(
    colname: String,
    metaType: String,
    mean: Double,
    std: Double,
    min_val: Double,
    co_of_var: Double,
    val_size_avg: Double,
    val_size_min: Double ) {

}


