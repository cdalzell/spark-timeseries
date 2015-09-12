/**
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.finance.parsers

import com.cloudera.sparkts.{TimeSeries, LocalSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{FunSuite, ShouldMatchers}

class CSVParserSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  test("CSV Parser - Google Finance") {
    val is = getClass.getClassLoader.getResourceAsStream("datafiles/google-finance/GOOG.csv")
    val lines = scala.io.Source.fromInputStream(is).getLines().toArray
    val text = lines.mkString("\n")
    val ts = GoogleParser.csvStringToTimeSeries(text)

    ts.data.rows should be (lines.length - 1)
  }

  test("CSV Parser - Quandl Finance") {
    val is = getClass.getClassLoader.getResourceAsStream("datafiles/quandl/WIKI-DATA.csv")
    val lines = scala.io.Source.fromInputStream(is).getLines().toArray
    val text = lines.mkString("\n")
    val ts = QuandlParser.csvStringToTimeSeries(text)

    ts.data.rows should be (lines.length - 1)
  }

  test("CSV Parser - Yahoo Finance") {
    val is = getClass.getClassLoader.getResourceAsStream("datafiles/yahoo-finance/YHOO.csv")
    val lines = scala.io.Source.fromInputStream(is).getLines().toArray
    val text = lines.mkString("\n")
    val ts = YahooParser.csvStringToTimeSeries(text)

    ts.data.rows should be (lines.length - 1)
  }

  test("CSV Parser - File Loader - Google Finance") {
    val dir = "datafiles/google-finance/"
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)

    sc = new SparkContext(conf)

    val seriesByFile: RDD[TimeSeries] = GoogleParser.loadFromCSVFiles(dir, sc)
  }
}
