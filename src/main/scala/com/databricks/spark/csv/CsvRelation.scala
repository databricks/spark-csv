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

import java.io.IOException

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.slf4j.LoggerFactory

import com.databricks.spark.csv.util.ParseMode

case class CsvRelation protected[spark] (
    location: String,
    useHeader: Boolean,
    delimiter: Char,
    quote: Char,
    parseMode: String,
    userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with InsertableRelation {

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  private val failFast = ParseMode.isFailFastMode(parseMode)
  private val dropMalformed = ParseMode.isDropMalformedMode(parseMode)
  private val permissive = ParseMode.isPermissiveMode(parseMode)

  val schema = inferSchema()

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  def buildScan = {
    val baseRDD = sqlContext.sparkContext.textFile(location)

    val numFields = schema.fields.length
    val row = new GenericMutableRow(numFields)
    val projection = schemaCaster(asAttributes(schema))
    val fieldNames = schema.fieldNames.toArray

    val csvFormat = CSVFormat.DEFAULT
      .withDelimiter(delimiter)
      .withQuote(quote)
      .withSkipHeaderRecord(false)
      .withHeader(fieldNames: _*)

    // If header is set, make sure firstLine is materialized before sending to executors.
    val filterLine = if (useHeader) firstLine else null

    baseRDD.mapPartitions { iter =>
      // When using header, any input line that equals firstLine is assumed to be header
      val csvIter = if (useHeader) {
        iter.filter(_ != filterLine)
      } else {
        iter
      }

      parseCSV(csvIter, csvFormat, schema.fields, projection, row)
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val csvFormat = CSVFormat.DEFAULT
        .withDelimiter(delimiter)
        .withQuote(quote)
        .withSkipHeaderRecord(false)
      val firstRow = CSVParser.parse(firstLine, csvFormat).getRecords.head.toList
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index"}
      }
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName =>
        StructField(fieldName.toString, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  /**
   * Returns the first line of the first non-empty file in path
   */
  private lazy val firstLine = {
    // Using Spark to read the first line to be able to handle all Hadoop input (gz, bz, etc.)
    sqlContext.sparkContext.textFile(location).first()
  }

  private def schemaCaster(sourceSchema: Seq[AttributeReference]): MutableProjection = {
    val startSchema = (1 to sourceSchema.length).toSeq.map(
      index => new AttributeReference(s"C$index", StringType, nullable = true)())
    val casts = sourceSchema.zipWithIndex.map { case (ar, i) => Cast(startSchema(i), ar.dataType) }
    new InterpretedMutableProjection(casts, startSchema)
  }

  private def parseCSV(
      iter: Iterator[String],
      csvFormat: CSVFormat,
      schemaFields: Seq[StructField],
      projection: MutableProjection,
      row: GenericMutableRow): Iterator[Row] = {
    iter.flatMap { line =>
      var index: Int = 0
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          logger.warn(s"Ignoring empty line: $line")
          None
        } else {
          val tokens = records.head
          index = 0
          if (dropMalformed && schemaFields.length != tokens.size) {
            logger.warn(s"Dropping malformed line: $line")
            None
          } else if (failFast && schemaFields.length != tokens.size) {
            throw new RuntimeException(s"Malformed line in FAILFAST mode: $line")
          } else {
            while (index < schemaFields.length) {
              row(index) = tokens.get(index)
              index = index + 1
            }
            Some(projection(row))
          }
        }
      } catch {
        case aiob: ArrayIndexOutOfBoundsException if permissive =>
          (index until schemaFields.length).foreach(ind => row(ind) = null)
          Some(projection(row))
        case NonFatal(e) if !failFast =>
          logger.error(s"Exception while parsing line: $line. ", e)
          None
      }
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean) = {
    val filesystemPath = new Path(location)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.
      data.saveAsCsvFile(location, Map("delimiter" -> delimiter.toString))
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }

  private def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(field => AttributeReference(field.name, field.dataType, nullable = true)())
  }
}
