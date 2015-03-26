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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Provides access to CSV data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
  }

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType) = {
    val path = checkPath(parameters)
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

    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag can be true or false")
    }

    CsvRelation(path, headerFlag, delimiterChar, quoteChar, schema)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      data.saveAsCsvFile(path, parameters)
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
