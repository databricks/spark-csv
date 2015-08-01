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


import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import com.databricks.spark.csv.util.{ParserLibs, ParseModes, TextFile}

/**
 * A collection of static functions for working with CSV files in Spark SQL
 */
class CsvParser {
  private var useHeader: Boolean = false
  private var csvParsingOpts: CSVParsingOpts = CSVParsingOpts()
  private var lineParsingOpts: LineParsingOpts = LineParsingOpts()
  private var realNumberParsingOpts: RealNumberParsingOpts = RealNumberParsingOpts()
  private var intNumberParsingOpts: IntNumberParsingOpts = IntNumberParsingOpts()
  private var stringParsingOpts: StringParsingOpts = StringParsingOpts()
  private var comment: Character = '#'
  private var schema: StructType = null
  private var parseMode: String = ParseModes.DEFAULT
  private var parserLib: String = ParserLibs.DEFAULT
  private var charset: String = TextFile.DEFAULT_CHARSET.name()
  private var inferSchema: Boolean = false

  def withUseHeader(flag: Boolean): CsvParser = {
    this.useHeader = flag
    this
  }

  def withDelimiter(delimiter: Character): CsvParser = {
    this.csvParsingOpts.delimiter = delimiter
    this
  }

  def withQuoteChar(quote: Character): CsvParser = {
    this.csvParsingOpts.quoteChar = quote
    this
  }

  def withSchema(schema: StructType): CsvParser = {
    this.schema = schema
    this
  }

  def withParseMode(mode: String): CsvParser = {
    this.parseMode = mode
    this
  }

  def withEscape(escapeChar: Character): CsvParser = {
    this.csvParsingOpts.escapeChar = escapeChar
    this
  }

  def withComment(commentChar: Character) : CsvParser = {
    this.comment = commentChar
    this
  }

  def withIgnoreLeadingWhiteSpace(ignore: Boolean): CsvParser = {
    this.csvParsingOpts.ignoreLeadingWhitespace = ignore
    this
  }

  def withIgnoreTrailingWhiteSpace(ignore: Boolean): CsvParser = {
    this.csvParsingOpts.ignoreTrailingWhitespace = ignore
    this
  }

  def withParserLib(parserLib: String): CsvParser = {
    this.parserLib = parserLib
    this
  }

  def withCsvParsingOpts(csvParsingOpts: CSVParsingOpts) = {
    this.csvParsingOpts = csvParsingOpts
    this
  }

  def withLineParsingOpts(lineParsingOpts: LineParsingOpts) = {
    this.lineParsingOpts = lineParsingOpts
    this
  }

  def withRealNumberParsingOpts(numberParsingOpts: RealNumberParsingOpts) = {
    this.realNumberParsingOpts = numberParsingOpts
    this
  }

  def withIntNumberParsingOpts(numberParsingOpts: IntNumberParsingOpts) = {
    this.intNumberParsingOpts = numberParsingOpts
    this
  }


  def withStringParsingOpts(stringParsingOpts: StringParsingOpts) = {
    this.stringParsingOpts = stringParsingOpts
    this
  }

  def withOpts(optMap: Map[String, String]) = {
    this.stringParsingOpts = StringParsingOpts(optMap)
    this.lineParsingOpts = LineParsingOpts(optMap)
    this.realNumberParsingOpts = RealNumberParsingOpts(optMap)
    this.intNumberParsingOpts = IntNumberParsingOpts(optMap)
    this.csvParsingOpts = CSVParsingOpts(optMap)
    this
  }

  def withCharset(charset: String): CsvParser = {
    this.charset = charset
    this
  }

  def withInferSchema(inferSchema: Boolean) = {
    this.inferSchema = inferSchema
    this
  }

  /** Returns a Schema RDD for the given CSV path. */
  @throws[RuntimeException]
  def csvFile(sqlContext: SQLContext, path: String): DataFrame = {
    val relation: CsvRelation = CsvRelation(
      path,
      useHeader,
      csvParsingOpts,
      parseMode,
      parserLib,
      schema,
      comment,
      lineParsingOpts,
      realNumberParsingOpts,
      intNumberParsingOpts,
      stringParsingOpts,
      charset,
      inferSchema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

}

