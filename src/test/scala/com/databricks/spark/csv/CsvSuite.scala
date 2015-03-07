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

import java.io.{IOException, File}

import org.apache.spark.sql.test._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class CsvSuite extends FunSuite {
  val carsFile = "src/test/resources/cars.csv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val emptyFile = "src/test/resources/empty.csv"
  val tempEmptyDir = "target/test/empty/"

  /**
   * This function deletes a file or a directory with everything that's in it. This function is
   * copied from Spark with minor modifications made to it. See original source at:
   * github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
   */
  private def deleteRecursively(file: File) {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  test("DSL test") {
    val results = TestSQLContext
      .csvFile(carsFile)
      .select("year")
      .collect()

    assert(results.size === 2)
  }

  test("DDL test") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE carsTable
        |USING com.databricks.spark.csv
        |OPTIONS (path "$carsFile", header "true")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === 2)
  }

  test("DSL test with alternative delimiter and quote") {
    val results = new CsvParser()
      .withDelimiter('|')
      .withQuoteChar('\'')
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === 2)
  }

  test("DDL test with alternative delimiter and quote") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === 2)
  }


  test("DSL test with empty file and known schema") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("column", StringType, false)))).withUseHeader(false)
      .csvFile(TestSQLContext, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DDL test with empty file") {
    sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$emptyFile", header "false")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    sql(s"""
        |CREATE TEMPORARY TABLE carsTable
        |(yearMade double, makeName string, modelName string, comments string, grp string)
        |USING com.databricks.spark.csv
        |OPTIONS (path "$carsFile", header "true")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT makeName FROM carsTable").collect().size === 2)
    assert(sql("SELECT avg(yearMade) FROM carsTable group by grp").collect().head(0) === 2004.5)
  }

  test("column names test") {
    val cars = new CsvParser()
      .withUseHeader(false)
      .csvFile(TestSQLContext, carsFile)
    assert(cars.schema.fields(0).name == "C0")
    assert(cars.schema.fields(2).name == "C2")
  }

  test("INSERT OVERWRITE test") {
    // Create a temp directory for table that will be overwritten
    deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    sql(
      s"""
        |CREATE TEMPORARY TABLE carsTableIO
        |USING com.databricks.spark.csv
        |OPTIONS (path "$carsFile", header "false")
      """.stripMargin.replaceAll("\n", " "))
    sql(s"""
        |CREATE TEMPORARY TABLE carsTableEmpty
        |(yearMade double, makeName string, modelName string, comments string, grp string)
        |USING com.databricks.spark.csv
        |OPTIONS (path "$tempEmptyDir", header "false")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM carsTableIO").collect().size === 3)
    assert(sql("SELECT * FROM carsTableEmpty").collect().isEmpty)

    sql(
      s"""
        |INSERT OVERWRITE TABLE carsTableEmpty
        |SELECT * FROM carsTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(sql("SELECT * FROM carsTableEmpty").collect().size == 3)
  }
}
