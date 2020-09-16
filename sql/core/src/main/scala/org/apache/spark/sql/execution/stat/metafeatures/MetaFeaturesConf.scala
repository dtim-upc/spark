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

object MetaFeaturesConf {





  /* ------------------------------------------------------------------
  |                Meta-features for all data types
  *------------------------------------------------------------------- */

  // done-write
  val Cardinality = MetaFeature("distinct_values_cnt").name("cardinality").dataType("all")
    .doc("Number of distinct values that appear within the column")
  val BestContainment = MetaFeature("bestContainment").name("bestContainment").dataType("all")
    .doc("bestContainment that can be achieved").dependant(true).normalize(true).normalizeType(5)

  // done-write
  val Uniqueness = MetaFeature("distinct_values_pct").name("uniqueness").dataType("all")
    .doc("Number of distinct values divided by the number of rows").dependant(true)
    .normalizeType(4)

  // done-write
  val Incompleteness = MetaFeature("missing_values_pct").name("incompleteness").dataType("all")
    .doc("Number of missing values").normalizeType(4)

  /* ------------------------------------------------------------------
  |                             Numeric
  *------------------------------------------------------------------- */

  //  done
  val MeanMF = MetaFeature("mean").name("mean").dataType("numeric")
    .doc("Average value of the column")
  //  done
  val StdMF = MetaFeature("std").name("std").dataType("numeric")
    .doc("Standard deviation of the column")
  //  done
  val MinMF = MetaFeature("min_val").name("min_val").dataType("numeric")
    .doc("Minimum value of the column")
  //  done
  val MaxMF = MetaFeature("max_val").name("max_val").dataType("numeric")
    .doc("Maximum value of the column")
  //  done
  val RangeMF = MetaFeature("range_val").name("range_val").dataType("numeric")
    .doc("range").dependant(true)
  //  done
  val CoVarMF = MetaFeature("co_of_var").name("co_of_var").dataType("numeric")
    .doc("co of variance").dependant(true)

  /* --------------------------  Histogram   -------------------------------- */

  val FirstQuartile = MetaFeature("q1").name("first_quartile").dataType("nominal")
    .doc("lower quartile")

  val SecondQuartile = MetaFeature("q2").name("second_quartile").dataType("nominal")
    .doc("middle quartile (median)")

  val ThirdQuartile = MetaFeature("q3").name("third_quartile").dataType("nominal")
    .doc("upper quartile")

  val maxDigits = MetaFeature("maxDigits").name("max_digits").dataType("numeric")
    .doc("Maximum number of digits")

  val maxDecimals = MetaFeature("maxDecimals").name("max_decimals").dataType("numeric")
    .doc("Maximum number of decimals")

  /* ------------------------------------------------------------------
  |                             Nominal
  *-------------------------------------------------------------------
  |                Frequency distribution metadata
  *------------------------------------------------------------------- */

//  val SoundexAVG = MetaFeature("soundex_avg").name("soundex_avg").dataType("nominal")
//    .doc("The average value of the frequency distribution count").useSoundexCnt(true)
//  val SoundexMin = MetaFeature("soundex_min").name("soundex_min").dataType("nominal")
//    .doc("The minimum value of the soundex distribution count").useSoundexCnt(true)
//  //  done
//  val SoundexMax = MetaFeature("soundex_max").name("soundex_max").dataType("nominal")
//    .doc("The maximum value of the frequency distribution count").useSoundexCnt(true)
//
//  //  done
//  val SoundexSD = MetaFeature("soundex_std").name("soundex_sd").dataType("nominal")
//    .doc("The standard deviation of the frequency distribution count").useSoundexCnt(true)
//
//
//  val Soundex1Q = MetaFeature("soundex_1q").name("soundex_1q").dataType("nominal")
//    .doc("Lower quartile of the frequency distribution count").useSoundexCnt(true).isQuartile(true)
//
//  // done
//  val Soundex2Q = MetaFeature("soundex_median").name("soundex_2q").dataType("nominal")
//    .doc("The median of the frequency distribution count").useSoundexCnt(true).isQuartile(true)
//
//  // done
//  val Soundex3Q = MetaFeature("soundex_3q").name("soundex_3q").dataType("nominal")
//    .doc("Upper quartile of the frequency distribution count").useSoundexCnt(true).isQuartile(true)
//
//  val SoundexIQR = MetaFeature("soundex_IQR").name("soundex_IQR").dataType("nominal")
//    .doc("Interquartile range of the frequency distribution count").dependant(true)

  //  done
  val FrequencyAVG = MetaFeature("val_size_avg").name("frequency_avg").dataType("nominal")
    .doc("The average value of the frequency distribution count").useFreqCnt(true)
  //  done
  val FrequencyMin = MetaFeature("val_size_min").name("frequency_min").dataType("nominal")
    .doc("The minimum value of the frequency distribution count").useFreqCnt(true)
  //  done
  val FrequencyMax = MetaFeature("val_size_max").name("frequency_max").dataType("nominal")
    .doc("The maximum value of the frequency distribution count").useFreqCnt(true)

  //  done
  val FrequencySD = MetaFeature("val_size_std").name("frequency_sd").dataType("nominal")
    .doc("The standard deviation of the frequency distribution count").useFreqCnt(true)

  // done
  val Frequency1Q = MetaFeature("frequency_1q").name("frequency_2qo").dataType("nominal")
    .doc("Lower quartile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Frequency2Q = MetaFeature("val_pct_median").name("frequency_4qo").dataType("nominal")
    .doc("The median of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Frequency3Q = MetaFeature("frequency_3q").name("frequency_6qo").dataType("nominal")
    .doc("Upper quartile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val FrequencyIQR = MetaFeature("frequency_IQR").name("frequency_IQR").dataType("nominal")
    .doc("Interquartile range of the frequency distribution count").dependant(true)



  // done
  val Frequency1Qo = MetaFeature("frequency_1qo").name("frequency_1qo").dataType("nominal")
    .doc("First octile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Frequency3Qo = MetaFeature("frequency_3qo").name("frequency_3qo").dataType("nominal")
    .doc("Third octile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Frequency5Qo = MetaFeature("frequency_5qo").name("frequency_5qo").dataType("nominal")
    .doc("fifth octile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Frequency7Qo = MetaFeature("frequency_7qo").name("frequency_7qo").dataType("nominal")
    .doc("Seventh octile of the frequency distribution count").useFreqCnt(true).isQuartile(true)
    .normalizeType(4)

  // done
  val Entropy = MetaFeature("entropy").name("entropy").dataType("nominal")
    .doc("The information entropy measures the distribution of an attribute")
    .isCustom(true) // .useFreqCnt(true)


  //  done
  val PctMinMF = MetaFeature("val_pct_min").name("val_pct_min").dataType("nominal")
    .doc("pct").dependant(true).normalizeType(4)
//  done
  val PctMaxMF = MetaFeature("val_pct_max").name("val_pct_max").dataType("nominal")
    .doc("pct").dependant(true).normalizeType(4)

// done
  val PctStdMF = MetaFeature("val_pct_std").name("val_pct_std").dataType("nominal")
    .doc("pct").useFreqCnt(true).normalizeType(4)

//  val PctMedianMF = MetaFeature("val_pct_median").name("val_pct_median").dataType("nominal")
//    .doc("pct median")


  // news meta-features


  // done
  val Constancy = MetaFeature("constancy").name("constancy").dataType("nominal")
    .doc("Frequency of most frequent value divided by number of rows").dependant(true)
    .normalizeType(4)

  // done - write
  val LongestWord = MetaFeature("longWord").name("len_max_word").dataType("nominal")
    .doc("The number of characters in the longest value in the column")
    .useFreqCnt(true).useRawVal(true)
  // done - write
  val ShortestWord = MetaFeature("shortWord").name("len_min_word").dataType("nominal")
    .doc("The number of characters in the shortest value in the column")
    .useFreqCnt(true).useRawVal(true)

  // done - write
  val AverageWord = MetaFeature("avgWord").name("len_avg_word").dataType("nominal")
    .doc("Average length of words in terms of characters")


  // done
  val isBinary = MetaFeature("binary").name("binary").dataType("nominal")
    .doc("Specifies 1 if the attribute is binary").dependant(true)



 // special metafeature that is hardcore to generate, so does not follow set options here
  val TypeInformation = MetaFeature("basicType").name("basicType").dataType("all")
      .doc("Classify column as numeric (0), alphabetic (1) ,alphanumeric (2), " +
        "date (3), time (4), binary(5)" +
        "phone, ips, url, emails").isCustom(true).normalize(false)

  val CardinalityAlphabetic = MetaFeature("numberAlphabetic").name("PctAlphabetic")
    .doc("Number of alphabetic tuples").isCustom(true).normalizeType(4)
  val CardinalityAlphanumeric = MetaFeature("numberAlphanumeric").name("PctAlphanumeric")
    .doc("Number of alphanumeric tuples").isCustom(true).normalizeType(4)
  val CardinalityNumeric = MetaFeature("numberNumeric").name("PctNumeric")
    .doc("Number of numeric tuples").isCustom(true).normalizeType(4)
  val CardinalityNonAlphanumeric = MetaFeature("numberNonAlphanumeric").name("PctNonAlphanumeric")
    .doc("Number of non-alphanumeric tuples").isCustom(true).normalizeType(4)
  val CardinalityDateTime = MetaFeature("numberDateTime").name("PctDateTime")
    .doc("Number of date time tuples").isCustom(true).normalizeType(4)

  // specific type

  val CardPhone = MetaFeature("cardPhone").name("PctPhones")
    .doc("Phones").isCustom(true).normalizeType(4)
  val cardEmail = MetaFeature("cardEmail").name("PctEmail")
    .doc("emails").isCustom(true).normalizeType(4)
  val cardIP = MetaFeature("cardIP").name("PctIP")
    .doc("pctIP").isCustom(true).normalizeType(4)
  val cardURL = MetaFeature("cardURL").name("PctURL")
    .doc("pctURL").isCustom(true).normalizeType(4)
  val cardUsername = MetaFeature("cardUsername").name("PctUsername")
    .doc("username").isCustom(true).normalizeType(4)
  val cardGeneral = MetaFeature("cardGeneral").name("PctGeneral")
    .doc("cardGeneral").isCustom(true).normalizeType(4)
  val cardSpaces = MetaFeature("cardSpaces").name("PctSpaces")
    .doc("").isCustom(true).normalizeType(4)
  val cardOthers = MetaFeature("cardOthers").name("PctOthers")
    .doc("").isCustom(true).normalizeType(4)
  val cardPhrases = MetaFeature("cardPhrases").name("PctPhrases")
    .doc("").isCustom(true).normalizeType(4)




//  val ShortestWord_NoBlank = MetaFeature("shortWord").name("len_min_word").dataType("nominal")
//    .doc("The number of characters in the shortest value in the column which is not blank")


  // !!!! distance will be edit distance

  // done - write
  val First_word = MetaFeature("firstWord").name("firstWord").dataType("nominal").normalizeType(1)
    .doc("The first string entry in a column that is sorted alphabetically")
    .useFreqCnt(true).useRawVal(true)

  // done - write
  val Last_word = MetaFeature("lastWord").name("lastWord").dataType("nominal").normalizeType(1)
    .doc("The last string entry in a column that is sorted alphabetically")
    .useFreqCnt(true).useRawVal(true)

  val numberWords = MetaFeature("words").name("numberWords").dataType("nominal").useWordCnt(true)
    .doc("Number of words in the attribute")
  val AvgWords = MetaFeature("AvgWords").name("wordsCntAvg").dataType("nominal").useWordCnt(true)
  val MinWords = MetaFeature("MinWords").name("wordsCntMin").dataType("nominal").useWordCnt(true)
  val MaxWords = MetaFeature("MaxWords").name("wordsCntMax").dataType("nominal").useWordCnt(true)
  val SdWords = MetaFeature("SdWords").name("wordsCntSd").dataType("nominal").useWordCnt(true)

  // TODO: handle normalize
  val Freq_word = MetaFeature("freqWord").name("freqWordContainment").dataType("nominal").normalizeType(2)
    .doc("The k most frequent string in a column overlapp").isCustom(true)  // .useFreqCnt(true)
  val Freq_wordSoundex = MetaFeature("freqWordSoundex").name("freqWordSoundexContainment")
    .dataType("nominal").normalizeType(2)
    .doc("The k most frequent string in a column overlapp").isCustom(true)

  val Freq_wordClean = MetaFeature("freqWordClean").name("freqWordCleanContainment")
    .dataType("nominal").normalizeType(3)
    .doc("The k most frequent string in a column overlapp").isCustom(true)// .useFreqCnt(true)

  val KWords = 20

  val zeroRatio = MetaFeature("zero ratio").name("zeroRatio").dataType("numeric")
    .doc("Percentage of 0 values")

// soundex code
// ngram
  val emptyMF = MetaFeature("isempty").name("isEmpty").dataType("all")
    .doc("to identify which columns are empty").dependant(true).normalize(false)



  val General = List(Cardinality, Uniqueness, Incompleteness, emptyMF)
  val Numeric = General ::: List(MeanMF, StdMF, MinMF, MaxMF, RangeMF, CoVarMF)
  val Nominal = General ::: List(FrequencyAVG, FrequencyMin, FrequencyMax, FrequencySD,
    PctMinMF, PctMaxMF, PctStdMF, Frequency1Q, Frequency2Q, Frequency3Q,
    Frequency1Qo, Frequency3Qo, Frequency5Qo, Frequency7Qo, LongestWord,
    ShortestWord, AverageWord, Entropy, First_word, Last_word, Freq_word, Freq_wordSoundex,
    FrequencyIQR, AvgWords, MinWords, MaxWords, SdWords, TypeInformation, numberWords,
    CardPhone, cardEmail, cardIP, cardURL, cardUsername, cardGeneral, cardOthers, cardSpaces,
    CardinalityAlphabetic, CardinalityAlphanumeric, CardinalityNumeric, cardPhrases,
    CardinalityNonAlphanumeric, CardinalityDateTime, isBinary, Freq_wordClean, BestContainment)
//    SoundexAVG, SoundexMin, SoundexMax, SoundexSD, Soundex1Q, Soundex2Q, Soundex3Q, SoundexIQR)

  val NominalDep = List(Uniqueness, RangeMF, CoVarMF, FrequencyIQR, PctMinMF,
    PctMaxMF, Constancy, Empty)



  val selectMetaFeatures = Nominal.filter(_.dependant != true).filter(_.isCustom != true)
//  val selectMetaFeaturesDep = Nominal.filter(_.dependant == true)
  val selectMetaFeaturesCustom = Nominal.filter(_.isCustom == true)
  val selectMetaFeaturesNomFeq = selectMetaFeatures.filter(_.useFreqCnt == true)
    .filter(_.isQuartile != true)
  val selectMetaFeaturesNom = selectMetaFeatures.filter(_.useFreqCnt !=true)





  private val Empty = "isEmpty" // when columns have 100% missing values



  val AlphabeticStr = "alphabetic"
  val DateTimeStr = "datetime"
  val AlphanumericStr = "alphanumeric"
  val NumericStr = "numeric"
  val NonAlphanumericStr = "nonAlphanumeric"

  val typesList = List(AlphabeticStr -> CardinalityAlphabetic.name,
    DateTimeStr -> CardinalityDateTime.name, AlphanumericStr -> CardinalityAlphanumeric.name,
    NumericStr -> CardinalityNumeric.name, NonAlphanumericStr -> CardinalityNonAlphanumeric.name)

  val phoneStr = "phone"
  val emailStr = "email"
  val ipStr = "ip"
  val urlStr = "url"
  val usernameStr = "username"
  val generalStr = "general"
  val otherStr = "other"
  val spacesStr = "space"
  val phrasesStr = "phrases"

  val specifyTypesList = List(phoneStr -> CardPhone.name, emailStr -> cardEmail.name,
    ipStr -> cardIP.name, urlStr -> cardURL.name, usernameStr -> cardUsername.name,
    generalStr -> cardGeneral.name, otherStr -> cardOthers.name, spacesStr -> cardSpaces.name,
    phrasesStr -> cardPhrases.name)

  /* ------------------------------------------------------------------
  |               REGEX
  *------------------------------------------------------------------- */
  lazy val NumericRGX = "[-]?[0-9]+[,.]?[0-9]*([\\/][0-9]+[,.]?[0-9]*)*".r
  lazy val EmailRGX = "^([a-z0-9_\\.\\+-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$".r
  lazy val AlphabeticRGX = "^[a-zA-Z]+$".r

  lazy val AlphanumericRGX = "^[a-zA-Z0-9]*$".r // with space
  lazy val AlphanumericPhrasesRGX = "^[a-zA-Z0-9 ]*$".r // with space
  lazy val UsernameRGX = "^[a-z0-9_-]{3,16}$".r // having a length of 3 to 16 characters
  lazy val URLRGX = "((mailto\\:|www\\.|(news|(ht|f)tp(s?))\\:\\/\\/){1}\\S+)".r

  lazy val IPRGX = ("^(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5" +
    "])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])$").r


  lazy val DateRGX = ("^(?:(?:31(\\/|-|\\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))" +
    "\\1|(?:(?:29|30)(\\/|-|\\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|" +
    "Dec))\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})$|^(?:29(\\/|-|\\.)(?:0?2|(?:Feb))\\3(?:(?:(?:1[6-" +
    "9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?" +
    ":0?[1-9]|1\\d|2[0-8])(\\/|-|\\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:" +
    "1[0-2]|(?:Oct|Nov|Dec)))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})$").r

  lazy val PhoneRGX = ("^(?:(?:\\(?(?:00|\\+)([1-4]\\d\\d|[1-9]\\d?)\\)?)?[\\-\\.\\ \\\\\\/]?)" +
    "?((?:\\(?\\d{1,}\\)?[\\-\\.\\ \\\\\\/]?){0,})(?:[\\-\\.\\ \\\\\\/]?(?:#|ext\\.?|extensio" +
    "n|x)[\\-\\.\\ \\\\\\/]?(\\d+))?$").r


  lazy val TimeRGX = ("^((([0]?[1-9]|1[0-2])(:|\\.)[0-5][0-9]((:|\\.)[0-5][0-9])?( )?(AM|am|aM|" +
    "Am|PM|pm|pM|Pm))|(([0]?[0-9]|1[0-9]|2[0-3])(:|\\.)[0-5][0-9]((:|\\.)[0-5][0-9])?))$").r
  lazy val DateTimeRGX = ("^((((([13578])|(1[0-2]))[\\-\\/\\s]?(([1-9])|([1-2][0-9])|(3[01])))|" +
    "((([469])|(11))[\\-\\/\\s]?(([1-9])|([1-2][0-9])|(30)))|(2[\\-\\/\\s]?(([1-9])|([1-2][0-9]" +
    "))))[\\-\\/\\s]?\\d{4})(\\s((([1-9])|(1[02]))\\:([0-5][0-9])((\\s)|(\\:([0-5][0-9])\\s))([" +
    "AM|PM|am|pm]{2,2})))?$").r

  lazy val NonAlphanumericRGX = "[^\\s\\p{L}\\p{N}]+".r






}


case class DataTypeMF (dataType: String, specificType: String )

