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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import org.apache.commons.csv._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{StructType, StructField, StringType}
import org.apache.spark.sql.sources.TableScan

import scala.collection.JavaConversions._

case class CsvRelation protected[spark] (
    location: String,
    useHeader: Boolean,
    delimiter: Char,
    quote: Char,
    userSchema: StructType = null)(@transient val sqlContext: SQLContext) extends TableScan {

  val schema = inferSchema()

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {
    val baseRDD = sqlContext.sparkContext.textFile(location)

    val numFields = schema.fields.length
    val row = new GenericMutableRow(numFields)
    val projection = schemaCaster(asAttributes(schema))
    val fieldNames = schema.fieldNames.toArray

    val csvFormat = CSVFormat.DEFAULT
      .withDelimiter(delimiter)
      .withSkipHeaderRecord(false)
      .withHeader(fieldNames: _*)


    baseRDD.mapPartitions { iter =>
      // When using header, any input line that equals firstLine is assumed to be header
      val csvIter = if (useHeader) {
        iter.filter(_ != firstLine)
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
      val firstRow = CSVParser.parse(firstLine, CSVFormat.DEFAULT).getRecords.head.toList

      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"V$index"}
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
  private val firstLine = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)

    val status = fs.getFileStatus(path)
    val singleFile = if (status.isDir) {
      fs.listStatus(path)
        .find(_.getLen > 0)
        .map(_.getPath)
        .getOrElse(sys.error(s"Could not find non-empty file at $path"))
    } else {
      path
    }

    val lineReader = new LineReader(fs.open(singleFile))
    val lineText = new Text()
    lineReader.readLine(lineText)
    lineReader.close()
    lineText.toString
  }

  private def schemaCaster(sourceSchema: Seq[AttributeReference]): MutableProjection = {
    val startSchema = (1 to sourceSchema.length).toSeq.map(
      index => new AttributeReference(s"V$index", StringType, nullable = true)())
    val casts = sourceSchema.zipWithIndex.map { case (ar, i) => Cast(startSchema(i), ar.dataType) }
    new InterpretedMutableProjection(casts, startSchema)
  }


  private def parseCSV(
      iter: Iterator[String],
      csvFormat: CSVFormat,
      schemaFields: Seq[StructField],
      projection: MutableProjection,
      row: GenericMutableRow): Iterator[Row] = {
    iter.map { line =>
      val tokens = CSVParser.parse(line, csvFormat).getRecords.head
      for (index <- 0 until schemaFields.length) {
        row(index) = tokens.get(index)
      }
      projection(row)
    }
  }

  private def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(field => AttributeReference(field.name, field.dataType, nullable = true)())
  }
}
