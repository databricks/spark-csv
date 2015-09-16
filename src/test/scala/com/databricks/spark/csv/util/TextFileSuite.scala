/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.csv.util

import java.nio.charset.UnsupportedCharsetException

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TextFileSuite extends FunSuite with BeforeAndAfterAll {
  val carsFile = "src/test/resources/cars.csv"
  val carsFile8859 = "src/test/resources/cars_iso-8859-1.csv"
  val numLines = 6
  val numColumns = 4
  val smallThorn = {
    // scalastyle:off
    '\u00fe' // non-ascii character
    // scalastyle:on
  }
  val utf8 = "utf-8"
  val iso88591 = "iso-8859-1"

  private var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext("local[2]", "TextFileSuite")
  }

  override def afterAll(): Unit = {
    try {
      sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("read utf-8 encoded file") {
    val baseRDD = TextFile.withCharset(sparkContext, carsFile, utf8)
    assert(baseRDD.count() === numLines)
    assert(baseRDD.first().count(_ == ',') == numColumns)
  }

  test("read utf-8 encoded file using charset alias") {
    val baseRDD = TextFile.withCharset(sparkContext, carsFile, "utf8")
    assert(baseRDD.count() === numLines)
    assert(baseRDD.first().count(_ == ',') == numColumns)
  }

  test("read iso-8859-1 encoded file") {
    val baseRDD = TextFile.withCharset(sparkContext, carsFile8859, iso88591)
    assert(baseRDD.count() === numLines)
    assert(baseRDD.first().count(_ == smallThorn) == numColumns)
  }

  test("read iso-8859-1 encoded file using charset alias") {
    val baseRDD = TextFile.withCharset(sparkContext, carsFile8859, "8859_1")
    assert(baseRDD.count() === numLines)
    assert(baseRDD.first().count(_ == smallThorn) == numColumns)
  }

  test("read iso-8859-1 encoded file with invalid charset") {
    val baseRDD = TextFile.withCharset(sparkContext, carsFile8859, utf8)
    assert(baseRDD.count() === numLines)
    // file loads but since it's not encoded in utf-8, non-ascii characters are not decoded
    // correctly.
    assert(baseRDD.first().count(_ == smallThorn) == 0)
  }

  test("unsupported charset") {
    val exception = intercept[UnsupportedCharsetException] {
      TextFile.withCharset(sparkContext, carsFile, "frylock").count()
    }
    assert(exception.getMessage.contains("frylock"))
  }
}
