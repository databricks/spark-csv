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
import java.nio.charset.UnsupportedCharsetException

import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._

/* Implicits */

import org.apache.spark.sql.test.TestSQLContext._

class CsvFastSuite extends FunSuite {
  val carsFile = "src/test/resources/cars.csv"
  val carsFile8859 = "src/test/resources/cars_iso-8859-1.csv"
  val carsTsvFile = "src/test/resources/cars.tsv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val nullNumbersFile = "src/test/resources/null-numbers.csv"
  val emptyFile = "src/test/resources/empty.csv"
  val escapeFile = "src/test/resources/escape.csv"
  val numbersFile = "src/test/resources/numbers.csv"
  val tempEmptyDir = "target/test/empty2/"

  val numCars = 3

  test("DSL test") {
    val results = TestSQLContext
      .csvFile(carsFile, parserLib = "univocity")
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test for iso-8859-1 encoded file") {
    val dataFrame = TestSQLContext
      .csvFile(carsFile8859, parserLib = "univocity", charset  = "iso-8859-1", delimiter = 'þ')

    assert(dataFrame.select("year").collect().size === numCars)

    val results = dataFrame.select("comment", "year").where(dataFrame("year") === "1997")
    assert(results.first.getString(0) === "Go get one now they are þoing fast")
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      val results = TestSQLContext
        .csvFile(carsFile8859, parserLib = "univocity", charset = "1-9588-osi")
        .select("year")
        .collect()
    }
    assert(exception.getMessage.contains("1-9588-osi"))
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

  test("DDL test with charset") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile8859", header "true", parserLib "univocity",
         |charset "iso-8859-1", delimiter "þ")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with tab separated file") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test parsing decimal type") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |(yearMade double, makeName string, modelName string, priceTag decimal,
         |comments string, grp string)
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT yearMade FROM carsTable").collect().size === numCars)
    assert(sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
  }

  test("DSL test for Line exception policy") {
    val results = new CsvParser()
      .withUseHeader(true)
      .withLineParsingOpts(LineParsingOpts(badLinePolicy = LineExceptionPolicy.Fill, fillValue = "fill"))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsFile)
      .collect()
    val volt = results.filter(row => row.get(2).asInstanceOf[String] == "Volt").head
    assert(volt.get(3).asInstanceOf[String] === "fill")
    assert(volt.get(4).asInstanceOf[String] === "fill")
  }

  test("DSL test for CSV Parsing Opts: delimiter") {
    val results = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(delimiter = '\t'))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsTsvFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test for CSV Parsing Opts: numParts") {
    val rdd = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(numParts = 1, delimiter = '\t'))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsTsvFile)
      .select("year")
      .rdd

    assert(rdd.partitions.size === 1)
  }

  test("DSL test for CSV Parsing Opts: quote") {
    val results = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(quoteChar = '\'', delimiter = '|', numParts = 1))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)

    val years = results.map(_.get(0).asInstanceOf[String])
    assert(years === Array("2012", "1997", "2015"))
  }

  test("DSL test for CSV Parsing Opts: whitespace") {
    var results = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(delimiter = '|', numParts = 1, ignoreLeadingWhitespace = false))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)

    var years = results.map(_.get(0).asInstanceOf[String])
    assert(years === Array(" \'2012\'", " 1997", "2015"))

    results = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(delimiter = '|', numParts = 1, ignoreLeadingWhitespace = false))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)

    years = results.map(_.get(0).asInstanceOf[String])
    assert(years === Array(" \'2012\'", " 1997", "2015"))

    results = new CsvParser()
      .withUseHeader(true)
      .withCsvParsingOpts(CSVParsingOpts(delimiter = '|', numParts = 1, ignoreTrailingWhitespace = false))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)

    years = results.map(_.get(0).asInstanceOf[String])
    assert(years === Array("\'2012\' ", "1997", "2015 "))
  }

  test("DSL test for CSV Parsing Opts: special") {
    var results = new CsvParser()
      .withUseHeader(true)
      .withSchema(StructType(Seq(StructField("double", DoubleType),
      StructField("float", FloatType),
      StructField("int", IntegerType),
      StructField("long", LongType))))
      .withCsvParsingOpts(CSVParsingOpts(numParts = 1))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, numbersFile)
      .collect()

    assert(results.size === 6)

    var doubles = results.map(_.get(0))
    assert(doubles.count(_.asInstanceOf[Double].isNaN) === 1)
    assert(doubles.count(_.asInstanceOf[Double].isInfinite) === 2)
    assert(doubles.count(_.asInstanceOf[Double] == 0.0) === 2)

    var floats = results.map(_.get(1))
    assert(floats.count(_.asInstanceOf[Float].isNaN) === 1)
    assert(floats.count(_.asInstanceOf[Float].isInfinite) === 2)
    assert(floats.count(_.asInstanceOf[Float] == 0.0) === 2)

    var ints = results.map(_.get(2))
    assert(ints.count(_.asInstanceOf[Int] == 0) === 2)

    var longs = results.map(_.get(3))
    assert(longs.count(_.asInstanceOf[Long] == 0) === 2)

   results = new CsvParser()
      .withUseHeader(true)
      .withSchema(StructType(Seq(StructField("double", DoubleType),
      StructField("float", FloatType),
      StructField("int", IntegerType),
      StructField("long", LongType))))
      .withCsvParsingOpts(CSVParsingOpts(numParts = 1))
      .withRealNumberParsingOpts(RealNumberParsingOpts(
          infNegStrings = ParsingOptions.defaultInfNegString + "-Inff"))
      .withParserLib("univocity")
      .csvFile(TestSQLContext, numbersFile)
      .collect()

    assert(results.size === 7)

    doubles = results.map(_.get(0))
    assert(doubles.count(_.asInstanceOf[Double].isNaN) === 1)
    assert(doubles.count(_.asInstanceOf[Double].isInfinite) === 3)
    assert(doubles.count(_.asInstanceOf[Double] == 0.0) === 2)

    floats = results.map(_.get(1))
    assert(floats.count(_.asInstanceOf[Float].isNaN) === 1)
    assert(floats.count(_.asInstanceOf[Float].isInfinite) === 3)
    assert(floats.count(_.asInstanceOf[Float] == 0.0) === 2)

    ints = results.map(_.get(2))
    assert(ints.count(_.asInstanceOf[Int] == 0) === 2)

    longs = results.map(_.get(3))
    assert(longs.count(_.asInstanceOf[Long] == 0) === 2)

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

    val exception = intercept[SparkException] {
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
      TestSQLContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = '|',
        quote = '\'',
        parserLib = "univocity")
        .select("year")
        .collect()

    assert(results.size === numCars)
  }

  test("Expect parsing error with wrong delimiter settting using sparkContext.csvFile") {
    intercept[ org.apache.spark.sql.AnalysisException] {
      TestSQLContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = ',',
        quote = '\'',
        parserLib = "univocity")
        .select("year")
        .collect()
    }
  }

  test("Expect wrong parsing results with wrong quote setting using sparkContext.csvFile") {
    val results =
      TestSQLContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = '|',
        quote = '"',
        parserLib = "univocity")
        .select("year")
        .collect()

    assert(results.slice(0, numCars).toSeq.map(_(0).asInstanceOf[String]) ==
      Seq(" '2012' ", " 1997", "2015 "))
  }

  test("DDL test with alternative delimiter and quote") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|",
         |parserLib "univocity")
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
    sql( s"""
            |CREATE TEMPORARY TABLE carsTable
            |(yearMade double, makeName string, modelName string, comments string, grp string)
            |USING com.databricks.spark.csv
            |OPTIONS (path "$emptyFile", header "false", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    sql( s"""
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
         |OPTIONS (path "$carsFile", header "true", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))
    sql( s"""
            |CREATE TEMPORARY TABLE carsTableEmpty
            |(yearMade double, makeName string, modelName string, comments string, grp string)
            |USING com.databricks.spark.csv
            |OPTIONS (path "$tempEmptyDir", header "false", parserLib "univocity")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM carsTableIO").collect().size === numCars)
    assert(sql("SELECT * FROM carsTableEmpty").collect().isEmpty)

    sql(
      s"""
         |INSERT OVERWRITE TABLE carsTableEmpty
         |SELECT * FROM carsTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(sql("SELECT * FROM carsTableEmpty").collect().size == numCars)
  }

  test("DSL save") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = TestSQLContext.csvFile(carsFile, parserLib = "univocity")
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "headerPerPart" -> "false"))

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
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "headerPerPart" -> "false"), classOf[GzipCodec])

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
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "headerPerPart" -> "false", "quote" -> "\""))

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
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "headerPerPart" -> "false", "quote" -> "!"))

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/", quote = '!', parserLib = "univocity")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with quoting, escaped quote") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "escape-copy.csv"

    val escape = TestSQLContext.csvFile(escapeFile, escape = '|', quote = '"')
    escape.saveAsCsvFile(copyFilePath, Map("header" -> "true", "headerPerPart" -> "false", "quote" -> "\""))

    val escapeCopy = TestSQLContext.csvFile(copyFilePath + "/", parserLib = "univocity")

    assert(escapeCopy.count == escape.count)
    assert(escapeCopy.collect.map(_.toString).toSet == escape.collect.map(_.toString).toSet)
    assert(escapeCopy.head().getString(0) == "\"thing")
  }

  test("DSL test schema inferred correctly") {

    val results = TestSQLContext
      .csvFile(carsFile, parserLib = "univocity", inferSchema = true)

    assert(results.schema == StructType(List(
      StructField("year",IntegerType,true),
      StructField("make",StringType,true),
      StructField("model",StringType,true),
      StructField("comment",StringType,true),
      StructField("blank",StringType,true))
    ))

    assert(results.collect().size === numCars)

  }

  test("DSL test inferred schema passed through") {

    val dataFrame = TestSQLContext
      .csvFile(carsFile, parserLib = "univocity", inferSchema = true)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.first.getString(0) === "No comment")
    assert(results.first.getInt(1) === 2012)

  }

  test("DDL test with inferred schema") {

    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "true", parserLib "univocity", inferSchema "true")
      """.stripMargin.replaceAll("\n", " "))

    val results = sql("select year from carsTable where make = 'Ford'")

    assert(results.first().getInt(0) === 1997)

  }

  test("DSL test nullable fields"){

    val results = new CsvParser()
      .withSchema(StructType(List(StructField("name", StringType, false), StructField("age", IntegerType, true))))
      .withUseHeader(true)
      .withParserLib("univocity")
      .csvFile(TestSQLContext, nullNumbersFile)
      .collect()

    assert(results.head.toSeq == Seq("alice", 35))
    assert(results(1).toSeq == Seq("bob", null))
    assert(results(2).toSeq == Seq("", 24))

  }
}
