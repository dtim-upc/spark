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

package org.apache.spark.sql.config

import org.apache.spark.sql.execution.stat.metafeatures.MetaFeature

class FindJoinsOptions {




  def getDefaultConfig(): Unit = {

  }

  val overrideMF = FindJoinOption("override.meta.features.computed")
    .doc("overrides meta-features already computed").value("false")

  val saveMF = FindJoinOption("save.meta.features.computed")
    .doc("save meta-features in disk. If not specified directory," +
      " metafeatures are saved in same location of dataframe").value("true")




  val options = new scala.collection.mutable.HashMap[String, String]

//  options += key -> value


}


case class FindJoinOption(key: String) {

  private var _doc: String = _
  private var _value: String = _

  def doc: String = _doc
  def value: String = _value


  def doc(d: String): FindJoinOption = {
    _doc = d
    this
  }

  def value(d: String): FindJoinOption = {
    _value = d
    this
  }
}