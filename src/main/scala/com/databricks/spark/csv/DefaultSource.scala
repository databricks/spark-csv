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
    val relation = CsvRelation(parameters("path"))(sqlContext)
    if (parameters.contains("delimiter")) {
      val delimiter = parameters("delimiter").charAt(0)
      relation.setDelimiter(delimiter)
    }
    if (parameters.contains("quote")) {
      val quoteChar = parameters("quote").charAt(0)
      relation.setQuoteChar(quoteChar)
    }
    if (parameters.contains("header")) {
      val headerFlag = parameters("header") == "true"
      relation.setUseHeader(headerFlag)
    }

    relation
  }
}