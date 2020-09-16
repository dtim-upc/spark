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
package org.apache.spark.sql.execution.stat.metafeatures

case class MetaFeature(key: String) {

  private var _doc: String = _
  private var _name: String = _
  private var _dataType: String = _
  private var _dependant: Boolean = false
  private var _useFreqCnt: Boolean = false
  private var _useSoundexCnt: Boolean = false
  private var _isQuartile: Boolean = false
  private var _normalize: Boolean = true
  // (0) distance, (1) editDistance, (2) containment, (3) containment clean
  // (4) distance without z-score
  private var _normalizeType: Int = 0
  private var _useRawVal: Boolean = false
  private var _isCustom: Boolean = false
  private var _useWordCnt: Boolean = false

//  def key: String = _key
  def doc: String = _doc
  def dataType: String = _dataType
  def name: String = _name
  def dependant: Boolean = _dependant
  def useFreqCnt: Boolean = _useFreqCnt
  def useSoundexCnt: Boolean = _useSoundexCnt
  def isQuartile: Boolean = _isQuartile
  def normalize: Boolean = _normalize
  def normalizeType: Int = _normalizeType
  def useRawVal: Boolean = _useRawVal
  def isCustom: Boolean = _isCustom
  def useWordCnt: Boolean = _useWordCnt


  def doc(d: String): MetaFeature = {
    _doc = d
    this
  }
  def normalizeType(d: Int): MetaFeature = {
    _normalizeType = d
    this
  }
  def useWordCnt(d: Boolean): MetaFeature = {
    _useWordCnt = d
    this
  }
  def useSoundexCnt(d: Boolean): MetaFeature = {
    _useSoundexCnt = d
    this
  }
  def useRawVal(d: Boolean): MetaFeature = {
    _useRawVal = d
    this
  }
  def isCustom(d: Boolean): MetaFeature = {
    _isCustom = d
    this
  }
  def normalize(d: Boolean): MetaFeature = {
    _normalize = d
    this
  }

  def dependant(d: Boolean): MetaFeature = {
    _dependant = d
    this
  }

  def useFreqCnt(d: Boolean): MetaFeature = {
    _useFreqCnt = d
    this
  }

  def isQuartile(d: Boolean): MetaFeature = {
    _isQuartile = d
    this
  }

  def name(n: String): MetaFeature = {
    _name = n
    this
  }

  def dataType(d: String): MetaFeature = {
    _dataType = d
    this
  }

}

