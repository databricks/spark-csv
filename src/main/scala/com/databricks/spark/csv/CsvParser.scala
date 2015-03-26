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

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.StructType

/**
 * A collection of static functions for working with CSV files in Spark SQL
 */
class CsvParser {

  private var useHeader: Boolean = true
  private var delimiter: Character = ','
  private var quote: Character = '"'
  private var escape: Character = null
  private var schema: StructType = null

  def withUseHeader(flag: Boolean): CsvParser = {
    this.useHeader = flag
    this
  }

  def withDelimiter(delimiter: Character): CsvParser = {
    this.delimiter = delimiter
    this
  }

  def withQuoteChar(quote: Character): CsvParser = {
    this.quote = quote
    this
  }

  def withEscapeChar(escape: Character): CsvParser = {
    this.escape = escape
    this
  }

  def withSchema(schema: StructType): CsvParser = {
    this.schema = schema
    this
  }

  /** Returns a Schema RDD for the given CSV path. */
  def csvFile(sqlContext: SQLContext, path: String): DataFrame = {
    val relation: CsvRelation = CsvRelation(path, useHeader, delimiter, quote, escape, schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

}

