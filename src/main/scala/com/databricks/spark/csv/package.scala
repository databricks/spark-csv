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

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.commons.lang3.StringUtils

package object csv {

  /**
   * Adds a method, `csvFile`, to SQLContext that allows reading CSV data.
   */
  implicit class CsvContext(sqlContext: SQLContext) {
    def csvFile(filePath: String,
                useHeader: Boolean = true,
                delimiter: Char = ',',
                quote: Char = '"',
                mode: String = "PERMISSIVE") = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        parseMode = mode)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }

    def tsvFile(filePath: String, useHeader: Boolean = true) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = '\t',
        quote = '"',
        parseMode = "PERMISSIVE")(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }

  implicit class CsvRow(row: Row) {
    def asQuotedSeq(quote: String): Seq[String] = {
      val seq: Seq[Any] = row.toSeq
      seq.map {
        _ match {
          case ref: AnyRef => {
            val escaped = StringUtils.replace(ref.toString, quote, s"\\$quote")
            s"$quote$escaped$quote"
          }
          case _ => quote + "null" + quote
        }
      }
    }

    def asCsvRow(delimiter: String, quote: Option[String]) = {
      quote match {
        case Some(q) => asQuotedSeq(q).mkString(delimiter)
        case None    => row.mkString(delimiter)
      }
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
      val quote = parameters.get("quote")
      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = if (generateHeader) {
        dataFrame.columns.map(c => s""""$c"""").mkString(delimiter)
      } else {
        "" // There is no need to generate header in this case
      }
      val strRDD = dataFrame.rdd.mapPartitions { iter =>
        new Iterator[String] {
          var firstRow: Boolean = generateHeader

          override def hasNext = iter.hasNext

          override def next: String = {
            val row = iter.next.asCsvRow(delimiter, quote)
            if (firstRow) {
              firstRow = false
              header + "\n" + row
            } else {
              row
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
