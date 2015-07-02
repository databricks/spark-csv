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

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.sql.{DataFrame, SQLContext}

package object csv {

  /**
   * Adds a method, `csvFile`, to SQLContext that allows reading CSV data.
   */
  implicit class CsvContext(sqlContext: SQLContext) {
    def csvFile(filePath: String,
                useHeader: Boolean = true,
                delimiter: Char = ',',
                quote: Char = '"',
                escape: Character = null,
                mode: String = "PERMISSIVE",
                parserLib: String = "COMMONS",
                ignoreLeadingWhiteSpace: Boolean = false,
                ignoreTrailingWhiteSpace: Boolean = false) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        parseMode = mode,
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }

    def tsvFile(filePath: String,
                useHeader: Boolean = true,
                parserLib: String = "COMMONS",
                ignoreLeadingWhiteSpace: Boolean = false,
                ignoreTrailingWhiteSpace: Boolean = false) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = '\t',
        quote = '"',
        escape = '\\',
        parseMode = "PERMISSIVE",
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }
  
  implicit class CsvSchemaRDD(dataFrame: DataFrame) {

    /**
     * Saves DataFrame as csv files. By default uses ',' as delimiter, and includes header line.
     */
    def saveAsCsvFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      // TODO(hossein): For nested types, we may want to perform special work
      val delimiter = parameters.getOrElse("delimiter", ",")
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

      val quoteChar = parameters.get("quote") match {
        case Some(s) => {
          if (s.length == 1) {
            Some(s.charAt(0))
          } else {
            throw new Exception("Quotation cannot be more than one character.")
          }
        }
        case None => None
      }

      val csvFormatBase = CSVFormat.DEFAULT
        .withDelimiter(delimiterChar)
        .withEscape(escapeChar)
        .withSkipHeaderRecord(false)
        .withNullString("null")

      val csvFormat = quoteChar match {
        case Some(c) => csvFormatBase.withQuote(c)
        case _ => csvFormatBase
      }

      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = if (generateHeader) {
        csvFormat.format(dataFrame.columns.map(_.asInstanceOf[AnyRef]):_*)
      } else {
        "" // There is no need to generate header in this case
      }

      val headerPerPart = parameters.getOrElse("headerPerPart", "true").toBoolean

      val strRDD = dataFrame.rdd.mapPartitionsWithIndex { case (index, iter) =>
        val csvFormatBase = CSVFormat.DEFAULT
          .withDelimiter(delimiterChar)
          .withEscape(escapeChar)
          .withSkipHeaderRecord(false)
          .withNullString("null")

        val csvFormat = quoteChar match {
          case Some(c) => csvFormatBase.withQuote(c)
          case _ => csvFormatBase
        }

        new Iterator[String] {
          var firstRow: Boolean = if(headerPerPart) generateHeader else generateHeader && index == 0

          override def hasNext = iter.hasNext || firstRow

          override def next: String = {
            if(!iter.isEmpty) {
              val row = csvFormat.format(iter.next.toSeq.map(_.asInstanceOf[AnyRef]):_*)
              if (firstRow) {
                firstRow = false
                header + csvFormat.getRecordSeparator() + row
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
