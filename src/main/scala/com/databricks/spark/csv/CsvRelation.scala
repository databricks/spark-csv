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
import org.apache.spark.sql.sources.{PrunedScan, BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types._
import com.databricks.spark.csv.readers.{BulkCsvReader, LineCsvReader}
import com.databricks.spark.csv.util._

case class CsvRelation protected[spark] (
    baseRDD: () => RDD[String],
    location: Option[String],
    useHeader: Boolean,
    delimiter: Char,
    quote: Character,
    escape: Character,
    comment: Character,
    parseMode: String,
    parserLib: String,
    ignoreLeadingWhiteSpace: Boolean,
    ignoreTrailingWhiteSpace: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType = null,
    inferCsvSchema: Boolean,
    codec: String = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with InsertableRelation {

  /**
   * Limit the number of lines we'll search for a header row that isn't comment-prefixed.
   */
  private val MAX_COMMENT_LINES_IN_HEADER = 10

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if ((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace) && ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  override val schema: StructType = inferSchema()

  private def tokenRdd(header: Array[String]): RDD[Array[String]] = {

    if (ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(baseRDD(), header)
    } else {
      val csvFormat = defaultCsvFormat
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withEscape(escape)
        .withSkipHeaderRecord(false)
        .withHeader(header: _*)
        .withCommentMarker(comment)

      // If header is set, make sure firstLine is materialized before sending to executors.
      val filterLine = if (useHeader) firstLine else null

      baseRDD().mapPartitions { iter =>
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

  override def buildScan: RDD[Row] = {
    val schemaFields = schema.fields
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

      if (dropMalformed && schemaFields.length != tokens.size) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      } else if (failFast && schemaFields.length != tokens.size) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      } else {
        var index: Int = 0
        val rowArray = new Array[Any](schemaFields.length)
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = TypeCast.castTo(tokens(index), field.dataType, field.nullable,
              treatEmptyValuesAsNulls)
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case nfe: java.lang.NumberFormatException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(",")}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse Exception. " +
              s"Dropping malformed line: ${tokens.mkString(",")}")
            None
        }
      }
    }
  }


  /**
   * This supports to eliminate unneeded columns before producing an RDD
   * containing all of its tuples as Row objects. This reads all the tokens of each line
   * and then drop unneeded tokens without casting and type-checking by mapping
   * both the indices produced by `requiredColumns` and the ones of tokens.
   */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val isTableScan = requiredColumns.isEmpty || schemaFields.deep == requiredFields.deep
    if (isTableScan) {
      buildScan
    } else {
      val requiredIndices = new Array[Int](requiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => requiredFields.contains(field)
      }.foreach {
        case(field, index) => requiredIndices(requiredFields.indexOf(field)) = index
      }

      val rowArray = new Array[Any](requiredIndices.length)
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>
        if (dropMalformed && schemaFields.length != tokens.size) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.size) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length != tokens.size) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < requiredIndices.length) {
              index = requiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls)
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray))
          } catch {
            case nfe: java.lang.NumberFormatException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse Exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = if (ParserLibs.isUnivocityLib(parserLib)) {
        val escapeVal = if (escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment
        val quoteChar: Char = if (quote == null) '\0' else quote
        new LineCsvReader(
          fieldSep = delimiter,
          quote = quoteChar,
          escape = escapeVal,
          commentMarker = commentChar).parseLine(firstLine)
      } else {
        val csvFormat = defaultCsvFormat
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
    if (comment == null) {
      baseRDD().first()
    } else {
      baseRDD().take(MAX_COMMENT_LINES_IN_HEADER)
        .find(! _.startsWith(comment.toString))
        .getOrElse(sys.error(s"No uncommented header line in " +
          s"first $MAX_COMMENT_LINES_IN_HEADER lines"))
    }
  }

  private def univocityParseCSV(
     file: RDD[String],
     header: Seq[String]): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val filterLine = if (useHeader) firstLine else null
    val dataLines = if (useHeader) file.filter(_ != filterLine) else file
    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        val escapeVal = if (escape == null) '\\' else escape.charValue()
        val commentChar: Char = if (comment == null) '\0' else comment
        val quoteChar: Char = if (quote == null) '\0' else quote

        new BulkCsvReader(iter, split,
          headers = header, fieldSep = delimiter,
          quote = quoteChar, escape = escapeVal, commentMarker = commentChar)
      }
    }, true)

    rows
  }

  private def parseCSV(
      iter: Iterator[String],
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
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val filesystemPath = location match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

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

      val codecClass = compresionCodecClass(codec)
      data.saveAsCsvFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString),
                         codecClass)
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
