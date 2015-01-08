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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

/**
 * Provides access to CSV data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val path = parameters("path")

    val delimiter = parameters.getOrElse("delimiter", ",")
    val delimiterChar = if (delimiter.length == 1) {
      delimiter.charAt(0)
    } else {
      throw new Exception("Delimiter cannot be more than one character.")
    }

    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar = if (quote.length == 1) {
      quote.charAt(0)
    } else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val useHeader = parameters.getOrElse("header", "true")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag can be true or false")
    }

    CsvRelation(path, headerFlag, delimiterChar, quoteChar)(sqlContext)
  }
}

