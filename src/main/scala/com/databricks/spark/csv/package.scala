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

import org.apache.spark.sql.{SQLContext, DataFrame}

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.StringWriter;

import scala.collection.convert.WrapAsJava

package object csv {

  /**
   * Adds a method, `csvFile`, to SQLContext that allows reading CSV data.
   */
  implicit class CsvContext(sqlContext: SQLContext) {
    def csvFile(filePath: String) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = true,
        delimiter = ',',
        quote = '"',
        escape = '\\')(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }

    def tsvFile(filePath: String) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = true,
        delimiter = '\t',
        quote = '"',
        escape = '\\')(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }

  implicit class CsvSchemaRDD(dataFrame: DataFrame) {
    def saveAsCsvFile(path: String, parameters: Map[String, String] = Map()): Unit = {
      // TODO(hossein): For nested types, we may want to perform special work
      val delimiter = parameters.getOrElse("delimiter", ",").charAt(0)
      val quote = parameters.getOrElse("quote", "\"").charAt(0)
      val escape = parameters.getOrElse("escape", "\\").charAt(0)
      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = dataFrame.columns

      var firstRow: Boolean = generateHeader
      val csvFileFormat = CSVFormat.DEFAULT
      .withDelimiter(delimiter)
      .withQuote(quote)
      .withEscape(escape)

      val strRDD = dataFrame.rdd.mapPartitions { iter =>
        var firstRow: Boolean = generateHeader
        val newIter = iter.map(_.toSeq.toArray)
        val stringWriter = new StringWriter()
        val csvPrinter = new CSVPrinter(stringWriter, csvFileFormat)
        if (firstRow) {
          firstRow = false
          csvPrinter.printRecord(header:_*)
        }
        csvPrinter.printRecords(WrapAsJava.asJavaIterable(newIter.toIterable))
        Iterator(stringWriter.toString)
      }
      strRDD.saveAsTextFile(path)
    }
  }
}
