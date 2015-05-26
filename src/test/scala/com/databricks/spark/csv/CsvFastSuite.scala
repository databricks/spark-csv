/*
 * Copyright 2015 Ayasdi Inc
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

import java.io.File

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.test._
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class CsvFastSuite extends FunSuite {
  val carsFile = "src/test/resources/cars.csv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val emptyFile = "src/test/resources/empty.csv"
  val escapeFile = "src/test/resources/escape.csv"
  val tempEmptyDir = "target/test/empty2/"

  val numCars = 3

  test("DSL test") {
    val results = TestSQLContext
      .csvFile(carsFile, parserLib = "univocity")
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DDL test") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "true", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DSL test for DROPMALFORMED parsing mode") {
    val results = new CsvParser()
      .withParseMode("DROPMALFORMED")
      .withUseHeader(true)
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars - 1)
  }

  test("DSL test for FAILFAST parsing mode") {
    val parser = new CsvParser()
      .withParseMode("FAILFAST")
      .withUseHeader(true)
      .withParserLib("univocity")

    val exception = intercept[SparkException]{
      parser.csvFile(TestSQLContext, carsFile)
        .select("year")
        .collect()
    }

    assert(exception.getMessage.contains("Malformed line in FAILFAST mode"))
  }


  test("DSL test with alternative delimiter and quote") {
    val results = new CsvParser()
      .withDelimiter('|')
      .withQuoteChar('\'')
      .withUseHeader(true)
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with alternative delimiter and quote using sparkContext.csvFile") {
    val results =
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = '|', quote = '\'', parserLib = "univocity")
        .select("year")
        .collect()

    assert(results.size === numCars)
  }

  test("Expect parsing error with wrong delimiter settting using sparkContext.csvFile") {
    intercept[ org.apache.spark.sql.AnalysisException] {
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = ',', quote = '\'', parserLib = "univocity")
        .select("year")
        .collect()
    }
  }

  test("Expect wrong parsing results with wrong quote setting using sparkContext.csvFile") {
    val results =
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = '|', quote = '"', parserLib = "univocity")
        .select("year")
        .collect()

    assert(results.slice(0, numCars).toSeq.map(_(0).asInstanceOf[String]) ==
      Seq("'2012'", "1997", "2015"))
  }

  test("DDL test with alternative delimiter and quote") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }


  test("DSL test with empty file and known schema") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("column", StringType, false))))
      .withUseHeader(false)
      .withParserLib("univocity")
      .csvFile(TestSQLContext, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DDL test with empty file") {
    sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$emptyFile", header "false", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$carsFile", header "true", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT makeName FROM carsTable").collect().size === numCars)
    assert(sql("SELECT avg(yearMade) FROM carsTable where grp = '' group by grp")
      .collect().head(0) === 2004.5)
  }

  test("DSL column names test") {
    val cars = new CsvParser()
      .withUseHeader(false)
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsFile)
    assert(cars.schema.fields(0).name == "C0")
    assert(cars.schema.fields(2).name == "C2")
  }

  test("SQL test insert overwrite") {
    // Create a temp directory for table that will be overwritten
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTableIO
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "false", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))
    sql(s"""
           |CREATE TEMPORARY TABLE carsTableEmpty
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$tempEmptyDir", header "false", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM carsTableIO").collect().size === numCars + 1)
    assert(sql("SELECT * FROM carsTableEmpty").collect().isEmpty)

    sql(
      s"""
         |INSERT OVERWRITE TABLE carsTableEmpty
         |SELECT * FROM carsTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(sql("SELECT * FROM carsTableEmpty").collect().size == numCars + 1)
  }

  test("DSL save") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = TestSQLContext.csvFile(carsFile, parserLib = "univocity")
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true"))

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with a compression codec") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = TestSQLContext.csvFile(carsFile, parserLib = "univocity")
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true"), classOf[GzipCodec])

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with quoting") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = TestSQLContext.csvFile(carsFile, parserLib = "univocity")
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "\""))

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/", parserLib = "univocity")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with alternate quoting") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = TestSQLContext.csvFile(carsFile)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "!"))

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/", quote = '!', parserLib = "univocity")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with quoting, escaped quote") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "escape-copy.csv"

    val escape = TestSQLContext.csvFile(escapeFile, escape='|', quote='"')
    escape.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "\""))

    val escapeCopy = TestSQLContext.csvFile(copyFilePath + "/", parserLib = "univocity")

    assert(escapeCopy.count == escape.count)
    assert(escapeCopy.collect.map(_.toString).toSet == escape.collect.map(_.toString).toSet)
    assert(escapeCopy.head().getString(0) == "\"thing")
  }
}
