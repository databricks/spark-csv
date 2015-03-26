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
package com.databricks.spark.csv;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;

/**
 * A collection of static functions for working with CSV files in Spark SQL
 */
public class JavaCsvParser {

  private Boolean useHeader = true;
  private Character delimiter = ',';
  private Character quote = '"';
  private Character escape = '\\';
  private StructType schema = null;

  public JavaCsvParser withUseHeader(Boolean flag) {
    this.useHeader = flag;
    return this;
  }

  public JavaCsvParser withDelimiter(Character delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  public JavaCsvParser withQuoteChar(Character quote) {
    this.quote = quote;
    return this;
  }

  public JavaCsvParser withEscapeChar(Character escape) {
    this.escape = escape;
    return this;
  }

  public JavaCsvParser withSchema(StructType schema) {
    this.schema = schema;
    return this;
  }

  /** Returns a Schema RDD for the given CSV path. */
  public DataFrame csvFile(SQLContext sqlContext, String path) {
    CsvRelation relation = new
            CsvRelation(path, useHeader, delimiter, quote, escape, schema, sqlContext);
    return sqlContext.baseRelationToDataFrame(relation);
  }
}
