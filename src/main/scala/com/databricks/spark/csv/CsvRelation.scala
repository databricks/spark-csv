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
package com.databricks.spark.csv

import java.io.IOException

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types._
import com.databricks.spark.csv.util._
import com.databricks.spark.sql.readers._

case class CsvRelation protected[spark](
   location: String,
   useHeader: Boolean,
   csvParsingOpts: CSVParsingOpts,
   parseMode: String,
   parserLib: String,
   userSchema: StructType = null,
   lineExceptionPolicy: LineParsingOpts = LineParsingOpts(),
   realNumOpts: RealNumberParsingOpts = RealNumberParsingOpts(),
   intNumOpts: IntNumberParsingOpts = IntNumberParsingOpts(),
   stringParsingOpts: StringParsingOpts = StringParsingOpts(),
   charset: String = TextFile.DEFAULT_CHARSET.name(),
   inferCsvSchema: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with InsertableRelation {

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if ((csvParsingOpts.ignoreLeadingWhitespace ||
    csvParsingOpts.ignoreTrailingWhitespace) &&
    ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options only supported with univocity parser option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  val schema = inferSchema()

  def tokenRdd(header: Array[String]): RDD[Array[String]] = {
    val baseRDD = TextFile.withCharset(sqlContext.sparkContext, location, charset,
      csvParsingOpts.numParts)

    if (ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD, header)
    } else {
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(csvParsingOpts.delimiter)
        .withQuote(csvParsingOpts.quoteChar)
        .withEscape(csvParsingOpts.escapeChar)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD.mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        } else {
          iter
        }
        parseCSV(csvIter, csvFormat)
      }
    }
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan = {
    val schemaFields = schema.fields
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>
      lazy val errorDetail = s"${tokens.mkString(csvParsingOpts.delimiter.toString)}"

      if (schemaFields.length != tokens.size &&
        (dropMalformed || lineExceptionPolicy.badLinePolicy == LineExceptionPolicy.Ignore)) {
        logger.warn(s"Dropping malformed line: $errorDetail")
        None
      } else if (schemaFields.length != tokens.size &&
        (failFast || lineExceptionPolicy.badLinePolicy == LineExceptionPolicy.Abort)) {
        throw new RuntimeException(s"Malformed line in FAILFAST or Abort mode: $errorDetail")
      } else {
        var index: Int = 0
        val rowArray = new Array[Any](schemaFields.length)
        try {
          index = 0
          while (index < schemaFields.length) {
            try {
              rowArray(index) = TypeCast.castTo(tokens(index), schemaFields(index).dataType)
            } catch {
              case e: NumberFormatException if realNumOpts.enable &&
                (schemaFields(index).dataType == DoubleType ||
                  schemaFields(index).dataType == FloatType) =>

                rowArray(index) = if (realNumOpts.nullStrings.contains(tokens(index))) {
                  null
                } else if (realNumOpts.nanStrings.contains(tokens(index))) {
                  TypeCast.castTo("NaN", schemaFields(index).dataType)
                } else if (realNumOpts.infPosStrings.contains(tokens(index))) {
                  TypeCast.castTo("Infinity", schemaFields(index).dataType)
                } else if (realNumOpts.infNegStrings.contains(tokens(index))) {
                  TypeCast.castTo("-Infinity", schemaFields(index).dataType)
                } else {
                  throw new IllegalStateException(
                    s"Failed to parse as double/float number ${tokens(index)}")
                }

              case _: NumberFormatException if intNumOpts.enable &&
                (schemaFields(index).dataType == IntegerType ||
                  schemaFields(index).dataType == LongType) =>

                rowArray(index) = if (intNumOpts.nullStrings.contains(tokens(index))) {
                  null
                } else {
                  throw new IllegalStateException(
                    s"Failed to parse as int/long number ${tokens(index)}")
                }
            }
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException
            if permissive || lineExceptionPolicy.badLinePolicy == LineExceptionPolicy.Fill =>
            (index until schemaFields.length).foreach { ind =>
              rowArray(ind) = TypeCast.castTo(lineExceptionPolicy.fillValue,
                schemaFields(index).dataType)
            }
            Some(Row.fromSeq(rowArray))
          case NonFatal(e) if !failFast =>
            logger.error(s"Exception while parsing line: $errorDetail. ", e)
            None
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = if (ParserLibs.isUnivocityLib(parserLib)) {
        val escapeVal = if (csvParsingOpts.escapeChar == null) '\\'
        else csvParsingOpts.escapeChar.charValue()
        new LineCsvReader(fieldSep = csvParsingOpts.delimiter,
          quote = csvParsingOpts.quoteChar,
          escape = escapeVal)
          .parseLine(firstLine)
      } else {
        val csvFormat = CSVFormat.DEFAULT
          .withDelimiter(csvParsingOpts.delimiter)
          .withQuote(csvParsingOpts.quoteChar)
          .withEscape(csvParsingOpts.escapeChar)
          .withSkipHeaderRecord(false)
        CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
      }
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
      if (this.inferCsvSchema) {
        InferSchema(tokenRdd(header), header)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    TextFile.withCharset(sqlContext.sparkContext, location, charset).first()
  }

  private def univocityParseCSV(file: RDD[String], header: Seq[String]) = {
    val dataLines = if (useHeader) {
      file.mapPartitionsWithIndex({
        case (partitionIndex, iter) => if (partitionIndex == 0) iter.drop(1) else iter
      }, true)
    }
    else {
      file
    }
    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        val escapeVal = if (csvParsingOpts.escapeChar == null) '\\'
        else csvParsingOpts.escapeChar.charValue()
        new BulkCsvReader(iter, split,
          headers = header, fieldSep = csvParsingOpts.delimiter,
          quote = csvParsingOpts.quoteChar, escape = escapeVal,
          ignoreLeadingSpace = csvParsingOpts.ignoreLeadingWhitespace,
          ignoreTrailingSpace = csvParsingOpts.ignoreTrailingWhitespace)
      }
    }, true)

    rows
  }

  private def parseCSV(iter: Iterator[String],
                       csvFormat: CSVFormat): Iterator[Array[String]] = {
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          Some(records.head.toArray)
        }
      } catch {
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean) = {
    val filesystemPath = new Path(location)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.
      data.saveAsCsvFile(location, Map("delimiter" -> csvParsingOpts.delimiter.toString))
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
