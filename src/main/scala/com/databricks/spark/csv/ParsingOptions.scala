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

  private[csv] def delimitedStringToSet(str: String) = {
    str.split(",").toSet
  }
}

/**
 * Options to control parsing of real numbers e.g. the types Float and Double
 * @param nanStrings NaNs
 * @param nullStrings nulls
 * @param infNegStrings negative infinity
 * @param infPosStrings positive infinity
 * @param enable false to not apply these options, default true
 */
case class RealNumberParsingOpts(
  var nanStrings: Set[String] = ParsingOptions.defaultNaNStrings,
  var infPosStrings: Set[String] = ParsingOptions.defaultInfPosString,
  var infNegStrings: Set[String] = ParsingOptions.defaultInfNegString,
  var nullStrings: Set[String] = ParsingOptions.defaultNullStrings,
  var enable: Boolean = true)

/**
 * Options to control parsing of integral numbers e.g. the types Int and Long
 * @param nullStrings nulls
 * @param enable false to not apply these options, default true
 */
case class IntNumberParsingOpts(var nullStrings: Set[String] = ParsingOptions.defaultNullStrings,
                                var enable: Boolean = true)

/**
 * Options to control parsing of strings
 * @param nullStrings nulls
 * @param emptyStringReplace replace empty string with this string
 */
case class StringParsingOpts(var emptyStringReplace: String = "",
                             var nullStrings: Set[String] = ParsingOptions.defaultNullStrings)

/**
 * options to handle exceptions while parsing a line
 * @param badLinePolicy abort, ignore line or fill with fillValue when not enough fields are parsed
 * @param fillValue if line exception policy is to fill in the blanks, use this value to fill
 */
case class LineParsingOpts(
  var badLinePolicy: LineExceptionPolicy.EnumVal = LineExceptionPolicy.Fill,
  var fillValue: String = null)

/**
 * CSV parsing options
 * @param quoteChar fields containing delimiters, other special chars are quoted using this
 *                  character e.g. "this is a comma ,"
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

/**
 * builds a [[RealNumberParsingOpts]] instance from "text"
 * realNumParsingOpts.{nans, infs, -infs, nulls, enable} are supported
 */
object RealNumberParsingOpts {
  val prefix = "realNumParsingOpts."
  val build = RealNumberParsingOpts()

  def apply(opts: Map[String, String]): RealNumberParsingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("nans", value: String) =>
          build.nanStrings = ParsingOptions.delimitedStringToSet(value)
        case ("infs", value: String) =>
          build.infPosStrings = ParsingOptions.delimitedStringToSet(value)
        case ("-infs", value: String) =>
          build.infNegStrings = ParsingOptions.delimitedStringToSet(value)
        case ("nulls", value: String) =>
          build.nullStrings = ParsingOptions.delimitedStringToSet(value)
        case ("enable", value: String) => build.enable = value.toBoolean
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[IntNumberParsingOpts]] instance from "text"
 * intNumParsingOpts.{nulls, enable} are supported
 */
object IntNumberParsingOpts {
  val prefix = "intNumParsingOpts."
  val build = IntNumberParsingOpts()

  def apply(opts: Map[String, String]): IntNumberParsingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("nulls", value: String) =>
          build.nullStrings = ParsingOptions.delimitedStringToSet(value)
        case ("enable", value: String) => build.enable = value.toBoolean
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[StringParsingOpts]] instance from "text"
 * stringParsingOpts.{nulls, emptyStringReplace} are supported
 */
object StringParsingOpts {
  val prefix = "stringParsingOpts."
  val build = StringParsingOpts()

  def apply(opts: Map[String, String]): StringParsingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("nulls", value: String) =>
          build.nullStrings = ParsingOptions.delimitedStringToSet(value)
        case ("emptyStringReplace", value: String) => build.emptyStringReplace = value
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[LineParsingOpts]] instance from "text"
 * lineParsingOpts.{badLinePolicy, fillValue} are supported
 * lineParsingOpts.badLinePolicy can be one of fill, ignore or abort
 */
object LineParsingOpts {
  val prefix = "lineParsingOpts."
  val build = LineParsingOpts()

  def apply(opts: Map[String, String]): LineParsingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("badLinePolicy", value: String) =>
          build.badLinePolicy = value.toLowerCase match {
            case "fill" => LineExceptionPolicy.Fill
            case "ignore" => LineExceptionPolicy.Ignore
            case "abort" => LineExceptionPolicy.Abort
            case _ => throw new IllegalArgumentException(s"Unknown option $opt")
          }
        case ("fillValue", value: String) => build.fillValue = value
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[CSVParsingOpts]] instance from "text"
 * csvParsingOpts.{delimiter, quote, escape, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace,
 *  numParts} are supported
 */
object CSVParsingOpts {
  val prefix = "csvParsingOpts."
  val build = CSVParsingOpts()

  def apply(opts: Map[String, String]): CSVParsingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("delimiter", value: String) => build.delimiter = value.charAt(0);
        case ("quote", value: String) => build.quoteChar = value.charAt(0)
        case ("escape", value: String) => build.escapeChar = value.charAt(0)
        case ("ignoreLeadingSpace", value: String) =>
          build.ignoreLeadingWhitespace = value.toBoolean
        case ("ignoreTrailingSpace", value: String) =>
          build.ignoreTrailingWhitespace = value.toBoolean
        case ("numParts", value: String) => build.numParts = value.toInt
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}
