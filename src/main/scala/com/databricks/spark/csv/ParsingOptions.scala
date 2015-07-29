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

/**
 * Action to take when malformed lines are found in a CSV File
 */
object LineExceptionPolicy {

  sealed trait EnumVal

  /**
   * ignore the malformed line and continue
   */
  case object Ignore extends EnumVal

  /**
   * stop parsing and abort
   */
  case object Abort extends EnumVal

  /**
   * if fields are missing in a line, fill in the blanks
   */
  case object Fill extends EnumVal

}

object ParsingOptions {
  val defaultNullStrings = HashSet("", "NULL", "N/A", "null", "n/a")
  val defaultNaNStrings = HashSet("NaN", "nan")
  val defaultInfPosString = HashSet("+Inf", "Inf", "Infinity", "+Infinity", "inf", "+inf")
  val defaultInfNegString = HashSet("-Inf", "-inf", "-Infinity")
}

/**
 * Options to control parsing of real numbers e.g. the types Float and Double
 * @param nanStrings these strings are NaNs
 * @param enable make this false to stop attempting to parse numbers i.e. treat them as strings
 */
case class RealNumberParsingOpts(var nanStrings: Set[String] =  ParsingOptions.defaultNaNStrings,
                                 var infPosStrings: Set[String] = ParsingOptions.defaultInfPosString,
                                 var infNegStrings: Set[String] = ParsingOptions.defaultInfNegString,
                                 var nullStrings: Set[String] = ParsingOptions.defaultNullStrings,
                                 var enable: Boolean = true)

/**
 * Options to control parsing of integral numbers e.g. the types Int and Long
 * @param enable make this false to stop attempting to parse numbers i.e. treat them as strings
 */
case class IntNumberParsingOpts(var nullStrings: Set[String] = ParsingOptions.defaultNullStrings,
                                var enable: Boolean = true)

/**
 * Options to control parsing of strings
 * @param emptyStringReplace replace empty string with this string
 */
case class StringParsingOpts(var emptyStringReplace: String = "",
                             var nullStrings: Set[String] =  ParsingOptions.defaultNullStrings)

/**
 * options to handle exceptions while parsing a line
 * @param badLinePolicy abort, ignore line or fill with nulls when a bad line is encountered
 * @param fillValue if line exception policy is to fill in the blanks, use this value to fill
 */
case class LineParsingOpts(var badLinePolicy: LineExceptionPolicy.EnumVal = LineExceptionPolicy.Fill,
                           var fillValue: String = "")

/**
 * CSV parsing options
 * @param quoteChar fields containing delimiters, other special chars are quoted using this character
 *                  e.g. "this is a comma ,"
 * @param escapeChar if a quote character appears in a field, it is escaped using this
 *                   e.g. "this is a quote \""
 * @param ignoreLeadingWhitespace ignore white space before a field
 * @param ignoreTrailingWhitespace ignore white space after a field
 * @param numParts number of partitions to use in sc.textFile()
 */
case class CSVParsingOpts(var delimiter: Character = ',',
                          var quoteChar: Character = '"',
                          var escapeChar: Character = '\\',
                          var ignoreLeadingWhitespace: Boolean = true,
                          var ignoreTrailingWhitespace: Boolean = true,
                          var numParts: Int = 0)
