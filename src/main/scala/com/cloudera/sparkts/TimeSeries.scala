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

package com.cloudera.sparkts

import breeze.linalg._
import breeze.numerics._
import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports._

import scala.collection.immutable.{IndexedSeq, Iterable}

class TimeSeries(val index: DateTimeIndex, val data: DenseMatrix[Double],
    val keys: Array[String]) extends Serializable {

  /**
   * IMPORTANT: currently this assumes that the DateTimeIndex is a UniformDateTimeIndex, not an
   * Irregular one. This means that this function won't work (yet) on TimeSeries built using
   * timeSeriesFromIrregularSamples().
   *
   * Lags all individual time series of the TimeSeries instance by up to maxLag amount.
   *
   * Example input TimeSeries:
   *   time 	a 	b
   *   4 pm 	1 	6
   *   5 pm 	2 	7
   *   6 pm 	3 	8
   *   7 pm 	4 	9
   *   8 pm 	5 	10
   *
   * With maxLag 2 and includeOriginals = true, we would get:
   *   time 	a 	lag1(a) 	lag2(a)  b 	lag1(b)  lag2(b)
   *   6 pm 	3 	2 	      1         8 	7 	      6
   *   7 pm 	4 	3 	      2         9 	8 	      7
   *   8 pm   5 	4 	      3         10	9 	      8
   *
   */
  def lags(maxLag: Int, includeOriginals: Boolean): TimeSeries = {
    val numCols = maxLag * keys.length + (if (includeOriginals) keys.length else 0)
    val numRows = data.rows - maxLag

    val laggedData = new DenseMatrix[Double](numRows, numCols)
    (0 until data.cols).foreach { colIndex =>
      val offset = maxLag + (if (includeOriginals) 1 else 0)
      val start = colIndex * offset

      Lag.lagMatTrimBoth(data(::, colIndex), laggedData(::, start to (start + offset - 1)), maxLag,
        includeOriginals)
    }

    val newKeys = keys.indices.map { keyIndex =>
      val key = keys(keyIndex)
      val lagKeys = (1 to maxLag).map(lagOrder => s"lag${lagOrder.toString}($key)").toArray

      if (includeOriginals) Array(key) ++ lagKeys else lagKeys
    }.reduce((prev: Array[String], next: Array[String]) => prev ++ next)

    val newDatetimeIndex = index.islice(maxLag, data.rows)

    new TimeSeries(newDatetimeIndex, laggedData, newKeys)
  }

  def slice(range: Range): TimeSeries = {
    new TimeSeries(index.islice(range), data(range, ::), keys)
  }

  def union(vec: Vector[Double], key: String): TimeSeries = {
    val mat = DenseMatrix.zeros[Double](data.rows, data.cols + 1)
    (0 until data.cols).foreach(c => mat(::, c to c) := data(::, c to c))
    mat(::, -1 to -1) := vec
    new TimeSeries(index, mat, keys :+ key)
  }

  /**
   * Returns a TimeSeries where each time series is differenced with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def differences(lag: Int): TimeSeries = {
    mapSeries(index.islice(lag, index.size), vec => diff(vec.toDenseVector, lag))
  }

  /**
   * Returns a TimeSeries where each time series is differenced with order 1. The new TimeSeries
   * will be missing the first date-time.
   */
  def differences(): TimeSeries = differences(1)

  /**
   * Returns a TimeSeries where each time series is quotiented with the given order. The new
   * TimeSeries will be missing the first n date-times.
   */
  def quotients(lag: Int): TimeSeries = {
    mapSeries(index.islice(lag, index.size), vec => UnivariateTimeSeries.quotients(vec, lag))
  }

  /**
   * Returns a TimeSeries where each time series is quotiented with order 1. The new TimeSeries will
   * be missing the first date-time.
   */
  def quotients(): TimeSeries = quotients(1)

  /**
   * Returns a return series for each time series. Assumes periodic (as opposed to continuously
   * compounded) returns.
   */
  def price2ret(): TimeSeries = {
    mapSeries(index.islice(1, index.size), vec => UnivariateTimeSeries.price2ret(vec, 1))
  }

  def univariateSeriesIterator(): Iterator[Vector[Double]] = {
    new Iterator[Vector[Double]] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): Vector[Double] = {
        i += 1
        data(::, i - 1)
      }
    }
  }

  def univariateKeyAndSeriesIterator(): Iterator[(String, Vector[Double])] = {
    new Iterator[(String, Vector[Double])] {
      var i = 0
      def hasNext: Boolean = i < data.cols
      def next(): (String, Vector[Double]) = {
        i += 1
        (keys(i - 1), data(::, i - 1))
      }
    }
  }

  /**
   * Applies a transformation to each series that preserves the time index.
   */
  def mapSeries(f: (Vector[Double]) => Vector[Double]): TimeSeries = {
    mapSeries(index, f)
  }

  /**
   * Applies a transformation to each series that preserves the time index. Passes the key along
   * with each series.
   */
  def mapSeriesWithKey(f: (String, Vector[Double]) => Vector[Double]): TimeSeries = {
    val newData = new DenseMatrix[Double](index.size, data.cols)
    univariateKeyAndSeriesIterator().zipWithIndex.foreach { case ((key, series), i) =>
      newData(::, i) := f(key, series)
    }
    new TimeSeries(index, newData, keys)
  }

  /**
   * Applies a transformation to each series such that the resulting series align with the given
   * time index.
   */
  def mapSeries(newIndex: DateTimeIndex, f: (Vector[Double]) => Vector[Double]): TimeSeries = {
    val newSize = newIndex.size
    val newData = new DenseMatrix[Double](newSize, data.cols)
    univariateSeriesIterator().zipWithIndex.foreach { case (vec, i) =>
      newData(::, i) := f(vec)
    }
    new TimeSeries(newIndex, newData, keys)
  }

  def mapValues[U](f: (Vector[Double]) => U): Seq[(String, U)] = {
    univariateKeyAndSeriesIterator().map(ks => (ks._1, f(ks._2))).toSeq
  }

  /**
   * Gets the first univariate series and its key.
   */
  def head(): (String, Vector[Double]) = univariateKeyAndSeriesIterator().next()
}

object TimeSeries {
  def timeSeriesFromIrregularSamples(samples: Seq[(DateTime, Array[Double])], keys: Array[String])
    : TimeSeries = {
    val mat = new DenseMatrix[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    for (i <- samples.indices) {
      val (dt, values) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new DenseVector[Double](values)
    }
    new TimeSeries(new IrregularDateTimeIndex(dts), mat, keys)
  }

  /**
   * This function should only be called when you can safely make the assumption that the time
   * samples are uniform (monotonously increasing) across time.
   */
  def timeSeriesFromUniformSamples(
      samples: Seq[Array[Double]],
      index: UniformDateTimeIndex,
      keys: Array[String]): TimeSeries = {
    val mat = new DenseMatrix[Double](samples.length, samples.head.length)

    for (i <- samples.indices) {
      mat(i to i, ::) := new DenseVector[Double](samples(i))
    }
    new TimeSeries(index, mat, keys)
  }
}

trait TimeSeriesFilter extends Serializable {
  /**
   * Takes a time series of i.i.d. observations and filters it to take on this model's
   * characteristics.
   * @param ts Time series of i.i.d. observations.
   * @param dest Array to put the filtered time series, can be the same as ts.
   * @return the dest param.
   */
  def filter(ts: Array[Double], dest: Array[Double]): Array[Double]
}
