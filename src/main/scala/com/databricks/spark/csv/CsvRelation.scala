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
import java.text.SimpleDateFormat

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
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
    codec: String = null,
    nullValue: String = "",
    dateFormat: String = null,
    maxCharsPerCol: Int = 100000)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with InsertableRelation {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

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
  private val superPermissive = ParseModes.isSuperPermissiveMode(parseMode)

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
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val rowArray = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

      if (dropMalformed && schemaFields.length != tokens.length) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      } else if (failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      } else if (superPermissive && schemaFields.length - 1 != tokens.length) {
        logger.warn("sudheer10 - " + schemaFields.length + " - " + tokens.length + " tokens - " + Row.fromSeq(tokens))
        Some(Row.fromSeq(new Array[String](schemaFields.length - 1) ++ Array(Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row")))))
      } else {
        val indexSafeTokens = if (superPermissive) {
          tokens ++ new Array[String](1)
        } else {
          tokens
        }
        var index: Int = 0
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = if (field.name == "__errors") {
              Row.fromSeq(Array(null, null))
            } else { TypeCast.castTo(indexSafeTokens(index), field.dataType, field.nullable,
              treatEmptyValuesAsNulls, nullValue, simpleDateFormatter)
            }
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case _: java.lang.NumberFormatException |
               _: IllegalArgumentException if(dropMalformed | superPermissive)=>
            if(dropMalformed) {
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            } else{
              Some(Row.fromSeq(new Array[String](schemaFields.length - 1) ++ Array(Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row")))))
            }
          case pe: java.text.ParseException if(dropMalformed | superPermissive) =>
            if(dropMalformed) {
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            } else {
              Some(Row.fromSeq(new Array[String](schemaFields.length - 1) ++ Array(Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row")))))
            }
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
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = schemaFields.deep == requiredFields.deep
    val safeRequiredFields = if (dropMalformed | superPermissive) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    } else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.length) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          } else if (permissive && schemaFields.length < tokens.length) {
            tokens.take(schemaFields.length)
          } else if (superPermissive && schemaFields.length - 1 != tokens.length) {
            new Array[String](schemaFields.length)
          } else if (superPermissive) {
            tokens ++ new Array[String](1)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = if (field.name == "__errors") {
                if (schemaFields.length - 1 != tokens.length) {
                  Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row"))
                } else {
                  Row.fromSeq(Array(null, null))
                }
              } else { TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls,
                nullValue,
                simpleDateFormatter
                )
              }
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case _: java.lang.NumberFormatException |
                 _: java.lang.NullPointerException |
                 _: IllegalArgumentException if(dropMalformed | superPermissive) =>
              if(dropMalformed) {
                logger.warn("Number format exception. " +
                  s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
                None
              } else{
                var subIndex: Int = 0
                while (subIndex < safeRequiredIndices.length) {
                  val field = schemaFields(safeRequiredIndices(subIndex))
                  rowArray(subIndex) = if (field.name == "__errors") {
                    Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row"))
                  } else {
                    null
                  }
                  subIndex = subIndex + 1
                }
                Some(Row.fromSeq(rowArray.take(requiredSize)))
              }
            case pe: java.text.ParseException if(dropMalformed | superPermissive) =>
              if(dropMalformed) {
                logger.warn("Parse exception. " +
                  s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
                None
              } else {
                var subIndex: Int = 0
                while (subIndex < safeRequiredIndices.length) {
                  val field = schemaFields(safeRequiredIndices(subIndex))
                  rowArray(subIndex) = if (field.name == "__errors") {
                    Row.fromSeq(Array(tokens.mkString(" , "), "Schema parse error: Malformed row"))
                  } else {
                    null
                  }
                  subIndex = subIndex + 1
                }
                Some(Row.fromSeq(rowArray.take(requiredSize)))
              }
          }
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    val errorSchema = StructType(Array(StructField("__row",StringType,true), StructField("__error_message",StringType,true)))
    if (this.userSchema != null) {
      if(superPermissive) {
        StructType(userSchema.fields ++ Array(StructField("__errors", errorSchema, true)))
      } else {
        userSchema
      }
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
        val simpleDateFormatter = dateFormatter
        InferSchema(
          tokenRdd(header),
          header,
          ParseModes.isDropMalformedMode(parseMode),
          nullValue,
          simpleDateFormatter)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        if(superPermissive) {
          StructType(schemaFields ++ Array(StructField("__errors", errorSchema, true)))
        } else {
          StructType(schemaFields)
        }
      }
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    if (comment != null) {
      baseRDD().filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment.toString)
      }.first()
    } else {
      baseRDD().filter { line =>
        line.trim.nonEmpty
      }.first()
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
          quote = quoteChar, escape = escapeVal,
          commentMarker = commentChar, maxCharsPerCol = maxCharsPerCol)
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

      val codecClass = CompressionCodecs.getCodecClass(codec)
      data.saveAsCsvFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString),
        codecClass)
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
