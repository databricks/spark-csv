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
package com.databricks.spark

import java.text.SimpleDateFormat
import java.sql.{Timestamp, Date}

import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.types.{DateType, TimestampType}

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.csv.util.TextFile

package object csv {

  val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  /**
   * Adds a method, `csvFile`, to SQLContext that allows reading CSV data.
   */
  implicit class CsvContext(sqlContext: SQLContext) extends Serializable{
    def csvFile(
        filePath: String,
        useHeader: Boolean = true,
        delimiter: Char = ',',
        quote: Char = '"',
        escape: Character = null,
        comment: Character = null,
        mode: String = "PERMISSIVE",
        parserLib: String = "COMMONS",
        ignoreLeadingWhiteSpace: Boolean = false,
        ignoreTrailingWhiteSpace: Boolean = false,
        charset: String = TextFile.DEFAULT_CHARSET.name(),
        inferSchema: Boolean = false): DataFrame = {
      val csvRelation = CsvRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        comment = comment,
        parseMode = mode,
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        treatEmptyValuesAsNulls = false,
        inferCsvSchema = inferSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }

    def tsvFile(
        filePath: String,
        useHeader: Boolean = true,
        parserLib: String = "COMMONS",
        ignoreLeadingWhiteSpace: Boolean = false,
        ignoreTrailingWhiteSpace: Boolean = false,
        charset: String = TextFile.DEFAULT_CHARSET.name(),
        inferSchema: Boolean = false): DataFrame = {
      val csvRelation = CsvRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        delimiter = '\t',
        quote = '"',
        escape = '\\',
        comment = '#',
        parseMode = "PERMISSIVE",
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        treatEmptyValuesAsNulls = false,
        inferCsvSchema = inferSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }

  implicit class CsvSchemaRDD(dataFrame: DataFrame) {

    /**
     * Saves DataFrame as csv files. By default uses ',' as delimiter, and includes header line.
     * If compressionCodec is not null the resulting output will be compressed.
     * Note that a codec entry in the parameters map will be ignored.
     */
    def saveAsCsvFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      // TODO(hossein): For nested types, we may want to perform special work
      val delimiter = parameters.getOrElse("delimiter", ",")
      // Before this change the csvFormatter wrote dates like this:
      // "2014-11-15 06:31:10.0", so have that as the default.
      val dateFormat = parameters.getOrElse("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
      val dateFormatter: SimpleDateFormat = new SimpleDateFormat(dateFormat)

      val delimiterChar = if (delimiter.length == 1) {
        delimiter.charAt(0)
      } else {
        throw new Exception("Delimiter cannot be more than one character.")
      }

      val escape = parameters.getOrElse("escape", null)
      val escapeChar: Character = if (escape == null) {
        null
      } else if (escape.length == 1) {
        escape.charAt(0)
      } else {
        throw new Exception("Escape character cannot be more than one character.")
      }

      val quote = parameters.getOrElse("quote", "\"")
      val quoteChar: Character = if (quote == null) {
        null
      } else if (quote.length == 1) {
        quote.charAt(0)
      } else {
        throw new Exception("Quotation cannot be more than one character.")
      }

      val quoteModeString = parameters.getOrElse("quoteMode", "MINIMAL")
      val quoteMode: QuoteMode = if (quoteModeString == null) {
        null
      } else {
        QuoteMode.valueOf(quoteModeString.toUpperCase)
      }

      val nullValue = parameters.getOrElse("nullValue", "null")

      val csvFormat = defaultCsvFormat
        .withDelimiter(delimiterChar)
        .withQuote(quoteChar)
        .withEscape(escapeChar)
        .withQuoteMode(quoteMode)
        .withSkipHeaderRecord(false)
        .withNullString(nullValue)

      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = if (generateHeader) {
        csvFormat.format(dataFrame.columns.map(_.asInstanceOf[AnyRef]): _*)
      } else {
        "" // There is no need to generate header in this case
      }

      // Create an index for the format by type so the type check
      // does not have to happen in the inner loop.
      val schema = dataFrame.schema
      val formatForIdx = schema.fieldNames.map(fname => schema(fname).dataType match {
        case TimestampType => (timestamp: Any) => {
          if (timestamp == null) {
            nullValue
          } else {
            dateFormatter.format(new Date(timestamp.asInstanceOf[Timestamp].getTime))
          }
        }
        case DateType => (date: Any) => {
          if (date == null) nullValue else dateFormatter.format(date)
        }
        case _ => (fieldValue: Any) => fieldValue.asInstanceOf[AnyRef]
      })

      val strRDD = dataFrame.rdd.mapPartitionsWithIndex { case (index, iter) =>
        val csvFormat = defaultCsvFormat
          .withDelimiter(delimiterChar)
          .withQuote(quoteChar)
          .withEscape(escapeChar)
          .withQuoteMode(quoteMode)
          .withSkipHeaderRecord(false)
          .withNullString(nullValue)

        new Iterator[String] {
          var firstRow: Boolean = generateHeader

          override def hasNext: Boolean = iter.hasNext || firstRow

          override def next: String = {
            if (iter.nonEmpty) {
              // try .zipWithIndex.foreach
              val values: Seq[AnyRef] = iter.next().toSeq.zipWithIndex.map {
                case (fieldVal, i) => formatForIdx(i)(fieldVal)
              }
              val row = csvFormat.format(values: _*)
              if (firstRow) {
                firstRow = false
                header + csvFormat.getRecordSeparator + row
              } else {
                row
              }
            } else {
              firstRow = false
              header
            }
          }
        }
      }
      compressionCodec match {
        case null => strRDD.saveAsTextFile(path)
        case codec => strRDD.saveAsTextFile(path, codec)
      }
    }
  }
}
