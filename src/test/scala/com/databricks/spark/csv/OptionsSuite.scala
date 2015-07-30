// scalastyle:off
/*
 * Copyright 2015 Ayasdi Inc
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
// scalastyle:on
package com.databricks.spark.csv

import scala.collection.immutable.HashSet

import org.scalatest.FunSuite

class OptionsSuite extends FunSuite {
  test("csv opts") {
    val optMap = Map("csvParsingOpts.delimiter" -> "|",
      "csvParsingOpts.quote" -> "[",
      "csvParsingOpts.ignoreLeadingSpace" -> "false",
      "csvParsingOpts.ignoreTrailingSpace" -> "true",
      "csvParsingOpts.escape" -> ":",
      "csvParsingOpts.numParts" -> "5")
    println(optMap)
    val opts = CSVParsingOpts(optMap)

    assert(opts.delimiter === '|')
    assert(opts.escapeChar === ':')
    assert(opts.ignoreLeadingWhitespace === false)
    assert(opts.ignoreTrailingWhitespace === true)
    assert(opts.numParts === 5)
    assert(opts.quoteChar === '[')
  }

  test("line opts") {
    val optMap = Map("lineParsingOpts.badLinePolicy" -> "abort",
      "lineParsingOpts.fillValue" -> "duh")
    println(optMap)
    val opts = LineParsingOpts(optMap)

    assert(opts.fillValue === "duh")
    assert(opts.badLinePolicy === LineExceptionPolicy.Abort)
  }

  test("string opts") {
    val optMap = Map("stringParsingOpts.nulls" -> "abcd,efg",
      "stringParsingOpts.emptyStringReplace" -> "<empty>")
    println(optMap)
    val opts = StringParsingOpts(optMap)

    assert(opts.nullStrings === HashSet("abcd", "efg"))
    assert(opts.emptyStringReplace === "<empty>")
  }

  test("int opts") {
    val optMap = Map("intNumParsingOpts.nulls" -> "abcd,efg",
      "intNumParsingOpts.enable" -> "false")
    println(optMap)
    val opts = IntNumberParsingOpts(optMap)

    assert(opts.nullStrings === HashSet("abcd", "efg"))
    assert(opts.enable === false)
  }

  test("real opts") {
    val optMap = Map("realNumParsingOpts.nulls" -> "abcd,efg",
      "realNumParsingOpts.enable" -> "false",
      "realNumParsingOpts.nans" -> "NaN,nan",
      "realNumParsingOpts.infs" -> "iinnff,IINNFF",
      "realNumParsingOpts.-infs" -> "minusInf")
    println(optMap)
    val opts = RealNumberParsingOpts(optMap)

    assert(opts.nullStrings === HashSet("abcd", "efg"))
    assert(opts.nanStrings === HashSet("NaN", "nan"))
    assert(opts.infPosStrings === HashSet("iinnff", "IINNFF"))
    assert(opts.infNegStrings === HashSet("minusInf"))
    assert(opts.enable === false)
  }

}
