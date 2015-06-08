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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.databricks.spark.csv.util.{ParserLibs, ParseModes, TypeCast}
import com.databricks.spark.sql.readers._

case class CsvRelation protected[spark] (
    location: String,
    useHeader: Boolean,
    delimiter: Char,
    quote: Char,
    escape: Character,
    parseMode: String,
    parserLib: String,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    userSchema: StructType = null,
    nullValues: Seq[String] = Seq(""))(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with InsertableRelation {

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace) && ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  val schema = inferSchema()

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan = {
    val baseRDD = sqlContext.sparkContext.textFile(location).map(line => line.replaceAll(nullValues.mkString("|"), ""))

    val fieldNames = schema.fieldNames

    if(ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD, fieldNames, schema.fields)
    } else {
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(fieldNames: _*)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD.mapPartitions { iter =>
        // When using header, any input line that equals firstLine is assumed to be header
        val csvIter = if (useHeader) {
          iter.filter(_ != filterLine)
        } else {
          iter
        }
        parseCSV(csvIter, csvFormat, schema.fields)
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = if(ParserLibs.isUnivocityLib(parserLib)) {
        val escapeVal = if(escape == null) '\\' else escape.charValue()
        new LineCsvReader(fieldSep = delimiter, quote = quote, escape = escapeVal)
          .parseLine(firstLine)
      } else {
        val csvFormat = CSVFormat.DEFAULT
          .withDelimiter(delimiter)
          .withQuote(quote)
          .withEscape(escape)
          .withSkipHeaderRecord(false)
        CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
      }
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName =>
        StructField(fieldName.toString, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    // Using Spark to read the first line to be able to handle all Hadoop input (gz, bz, etc.)
    sqlContext.sparkContext.textFile(location).first()
  }

  private def univocityParseCSV(
     file: RDD[String],
     header: Seq[String],
     schemaFields: Seq[StructField]) = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val filterLine = if (useHeader) firstLine else null
    val dataLines = if(useHeader) file.filter(_ != filterLine) else file
    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        val escapeVal = if(escape == null) '\\' else escape.charValue()
        new BulkCsvReader(iter, split,
          headers = header, fieldSep = delimiter,
          quote = quote, escape = escapeVal).flatMap { tokens =>
          if (dropMalformed && schemaFields.length != tokens.size) {
            logger.warn(s"Dropping malformed line: $tokens")
            None
          } else if (failFast && schemaFields.length != tokens.size) {
            throw new RuntimeException(s"Malformed line in FAILFAST mode: $tokens")
          } else {
            var index: Int = 0
            val rowArray = new Array[Any](schemaFields.length)
            try {
              index = 0
              while (index < schemaFields.length) {
                rowArray(index) = TypeCast.castTo(tokens(index), schemaFields(index).dataType)
                index = index + 1
              }
              Some(Row.fromSeq(rowArray))
            } catch {
              case aiob: ArrayIndexOutOfBoundsException if permissive =>
                (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
                Some(Row.fromSeq(rowArray))
              case NonFatal(e) if !failFast =>
                logger.error(s"Exception while parsing line: $tokens. ", e)
                None
            }
          }
        }
      }
    }, true)

    rows
  }

  private def parseCSV(
      iter: Iterator[String],
      csvFormat: CSVFormat,
      schemaFields: Seq[StructField]): Iterator[Row] = {
    iter.flatMap { line =>
      var index: Int = 0
      val rowArray = new Array[Any](schemaFields.length)
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          val tokens = records.head
          index = 0
          if (dropMalformed && schemaFields.length != tokens.size) {
            logger.warn(s"Dropping malformed line: $line")
            None
          } else if (failFast && schemaFields.length != tokens.size) {
            throw new RuntimeException(s"Malformed line in FAILFAST mode: $line")
          } else {
            while (index < schemaFields.length) {
              rowArray(index) = TypeCast.castTo(tokens.get(index), schemaFields(index).dataType)
              index = index + 1
            }
            Some(Row.fromSeq(rowArray))
          }
        }
      } catch {
        case aiob: ArrayIndexOutOfBoundsException if permissive =>
          (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
          Some(Row.fromSeq(rowArray))
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
      data.saveAsCsvFile(location, Map("delimiter" -> delimiter.toString))
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
