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

import java.io.File
import java.nio.charset.UnsupportedCharsetException

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.Row
import org.apache.spark.sql.test._
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class CsvSuite extends FunSuite {
  val carsFile = "src/test/resources/cars.csv"
  val carsFile8859 = "src/test/resources/cars_iso-8859-1.csv"
  val carsTsvFile = "src/test/resources/cars.tsv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val nullNumbersFile = "src/test/resources/null-numbers.csv"
  val emptyFile = "src/test/resources/empty.csv"
  val escapeFile = "src/test/resources/escape.csv"
  val tempEmptyDir = "target/test/empty/"
  val commentsFile = "src/test/resources/comments.csv"
  val disableCommentsFile = "src/test/resources/disable_comments.csv"

  val numCars = 3

  test("DSL test") {
    val results = TestSQLContext
      .csvFile(carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test for iso-8859-1 encoded file") {
    val dataFrame = new CsvParser()
      .withUseHeader(true)
      .withCharset("iso-8859-1")
      .withDelimiter('þ')
      .csvFile(TestSQLContext, carsFile8859)

    assert(dataFrame.select("year").collect().size === numCars)

    val results = dataFrame.select("comment", "year").where(dataFrame("year") === "1997")
    assert(results.first.getString(0) === "Go get one now they are þoing fast")
  }

  test("DSL test for bad charset name") {
    val parser = new CsvParser()
      .withUseHeader(true)
      .withCharset("1-9588-osi")

    val exception = intercept[UnsupportedCharsetException] {
      parser.csvFile(TestSQLContext, carsFile)
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
        |OPTIONS (path "$carsFile", header "true")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with tab separated file") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test parsing decimal type") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |(yearMade double, makeName string, modelName string, priceTag decimal,
         | comments string, grp string)
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT yearMade FROM carsTable").collect().size === numCars)
    assert(sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
  }


  test("DSL test for DROPMALFORMED parsing mode") {
    val results = new CsvParser()
      .withParseMode("DROPMALFORMED")
      .withUseHeader(true)
      .csvFile(TestSQLContext, carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars - 1)
  }

  test("DSL test for FAILFAST parsing mode") {
    val parser = new CsvParser()
      .withParseMode("FAILFAST")
      .withUseHeader(true)

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
      .csvFile(TestSQLContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with alternative delimiter and quote using sparkContext.csvFile") {
    val results =
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = '|', quote = '\'')
      .select("year")
      .collect()

    assert(results.size === numCars)
  }


  test("Expect parsing error with wrong delimiter setting using sparkContext.csvFile") {
    intercept[ org.apache.spark.sql.AnalysisException] {
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = ',', quote = '\'')
        .select("year")
        .collect()
    }
  }

  test("Expect wrong parsing results with wrong quote setting using sparkContext.csvFile") {
    val results =
      TestSQLContext.csvFile(carsAltFile, useHeader = true, delimiter = '|', quote = '"')
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
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with charset") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile8859", header "true", delimiter "þ", charset "iso-8859-1")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === numCars)
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

    assert(sql("SELECT makeName FROM carsTable").collect().size === numCars)
    assert(sql("SELECT avg(yearMade) FROM carsTable where grp = '' group by grp")
      .collect().head(0) === 2004.5)
  }

  test("DSL column names test") {
    val cars = new CsvParser()
      .withUseHeader(false)
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
        |OPTIONS (path "$carsFile", header "true")
      """.stripMargin.replaceAll("\n", " "))
    sql(s"""
        |CREATE TEMPORARY TABLE carsTableEmpty
        |(yearMade double, makeName string, modelName string, comments string, grp string)
        |USING com.databricks.spark.csv
        |OPTIONS (path "$tempEmptyDir", header "false")
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

    val cars = TestSQLContext.csvFile(carsFile)
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

    val cars = TestSQLContext.csvFile(carsFile)
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

    val cars = TestSQLContext.csvFile(carsFile)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "\""))

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/")

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

    val carsCopy = TestSQLContext.csvFile(copyFilePath + "/", quote = '!')

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

    val escapeCopy = TestSQLContext.csvFile(copyFilePath + "/")

    assert(escapeCopy.count == escape.count)
    assert(escapeCopy.collect.map(_.toString).toSet == escape.collect.map(_.toString).toSet)
    assert(escapeCopy.head().getString(0) == "\"thing")
  }


  test("DSL test schema inferred correctly") {

    val results = new CsvParser()
      .withInferSchema(true)
      .withUseHeader(true)
      .csvFile(TestSQLContext, carsFile)

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
      .csvFile(carsFile, inferSchema = true)

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
         |OPTIONS (path "$carsFile", header "true", inferSchema "true")
      """.stripMargin.replaceAll("\n", " "))

    val results = sql("select year from carsTable where make = 'Ford'")

    assert(results.first().getInt(0) === 1997)

  }

  test("DSL test nullable fields"){

    val results = new CsvParser()
      .withSchema(StructType(List(StructField("name", StringType, false), StructField("age", IntegerType, true))))
      .withUseHeader(true)
      .csvFile(TestSQLContext, nullNumbersFile)
      .collect()

    assert(results.head.toSeq == Seq("alice", 35))
    assert(results(1).toSeq == Seq("bob", null))
    assert(results(2).toSeq == Seq("", 24))

  }

  test("Commented lines in CSV data") {
    val results: Array[Row] = new CsvParser()
      .withDelimiter(',')
      .withComment('~')
      .csvFile(TestSQLContext, commentsFile)
      .collect()

    val expected =
      Seq(Seq("1", "2", "3", "4", "5"),
          Seq("6", "7", "8", "9", "0"),
          Seq("1", "2", "3", "4", "5"))

    assert(results.toSeq.map(_.toSeq) == expected)
  }

  test("Setting commment to null disables comment support") {
    val results: Array[Row] = new CsvParser()
      .withDelimiter(',')
      .withComment(null)
      .csvFile(TestSQLContext, disableCommentsFile)
      .collect()

    val expected =
      Seq(
        Seq("#1", "2", "3"),
        Seq("4", "5", "6"))

    assert(results.toSeq.map(_.toSeq) == expected)
  }

  test("DSL load csv from rdd") {

    val csvRdd = TestSQLContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))
    val df = new CsvParser().withUseHeader(true).csvRdd(TestSQLContext, csvRdd).collect()

    assert(df(0).toSeq == Seq("20", "1.8"))
    assert(df(1).toSeq == Seq("16", "1.7"))

  }

  test("Inserting into csvRdd should throw exception"){

    val csvRdd = TestSQLContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))
    val sampleData = TestSQLContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))

    val df = new CsvParser().withUseHeader(true).csvRdd(TestSQLContext, csvRdd)
    val sampleDf = new CsvParser().withUseHeader(true).csvRdd(TestSQLContext, sampleData)

    df.registerTempTable("csvRdd")
    sampleDf.registerTempTable("sampleDf")

    val exception = intercept[java.io.IOException] {
      sql("INSERT OVERWRITE TABLE csvRdd select * from sampleDf")
    }
    assert(exception.getMessage.contains("Cannot INSERT into table with no path defined"))
  }
}