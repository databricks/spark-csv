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
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.io.Source

import com.databricks.spark.csv.util.ParseModes
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

abstract class AbstractCsvSuite extends FunSuite with BeforeAndAfterAll {
  val carsFile = "src/test/resources/cars.csv"
  val carsMalformedFile = "src/test/resources/cars-malformed.csv"
  val carsFile8859 = "src/test/resources/cars_iso-8859-1.csv"
  val carsTsvFile = "src/test/resources/cars.tsv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val carsUnbalancedQuotesFile = "src/test/resources/cars-unbalanced-quotes.csv"
  val nullNumbersFile = "src/test/resources/null-numbers.csv"
  val nullNullNumbersFile = "src/test/resources/null_null_numbers.csv"
  val nullSlashNNumbersFile = "src/test/resources/null_slashn_numbers.csv"
  val emptyFile = "src/test/resources/empty.csv"
  val ageFile = "src/test/resources/ages.csv"
  val ageFileAlternative = "src/test/resources/ages-alternative.csv"
  val ageFileAlternativeMalformed = "src/test/resources/ages-alternative-malformed.csv"
  val escapeFile = "src/test/resources/escape.csv"
  val tempEmptyDir = "target/test/empty/"
  val commentsFile = "src/test/resources/comments.csv"
  val disableCommentsFile = "src/test/resources/disable_comments.csv"
  val boolFile = "src/test/resources/bool.csv"
  val datesFile = "src/test/resources/dates.csv"
  val longColsFile = "src/test/resources/long-cols.csv"
  private val simpleDatasetFile = "src/test/resources/simple.csv"

  val numCars = 3
  val numAges = 8
  val numMalformedAges = 6

  protected def parserLib: String

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "CsvSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("DSL test") {
    val results = sqlContext
      .csvFile(carsFile, parserLib = parserLib)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test for iso-8859-1 encoded file") {
    // scalastyle:off
    val dataFrame = sqlContext
      .csvFile(carsFile8859, parserLib = parserLib, charset = "iso-8859-1", delimiter = 'þ')

    assert(dataFrame.select("year").collect().size === numCars)

    val results = dataFrame.select("comment", "year").where(dataFrame("year") === "1997")
    assert(results.first.getString(0) === "Go get one now they are þoing fast")
    // scalastyle:on
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      val results = sqlContext
        .csvFile(carsFile8859, parserLib = parserLib, charset = "1-9588-osi")
        .select("year")
        .collect()
    }
    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("DDL test") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "true", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with alias name") {
    assume(org.apache.spark.SPARK_VERSION.take(3) >= "1.5",
      "Datasource alias feature was added in Spark 1.5")

    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTsvTable
         |USING csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with charset") {
    // scalastyle:off
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile8859", header "true", parserLib "$parserLib",
         |charset "iso-8859-1", delimiter "þ")
      """.stripMargin.replaceAll("\n", " "))
    //scalstyle:on

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with tab separated file") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test parsing decimal type") {
    assume(org.apache.spark.SPARK_VERSION.take(3) > "1.3",
      "DecimalType is broken on Spark 1.3.x")
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |(yearMade double, makeName string, modelName string, priceTag decimal,
         | comments string, grp string)
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsTsvFile", header "true", delimiter "\t", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT yearMade FROM carsTable").collect().size === numCars)
    assert(
      sqlContext.sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
  }

  test("DSL test for DROPMALFORMED parsing mode") {
    val results = new CsvParser()
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvFile(sqlContext, carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars - 1)
  }

  test("DSL test for DROPMALFORMED parsing mode with pruned scan") {
    val strictSchema = new StructType(
      Array(
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("Height", DoubleType, true),
        StructField("Born", TimestampType, true)
      )
    )

    val results = new CsvParser()
      .withSchema(strictSchema)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .csvFile(sqlContext, ageFile)
      .select("Name")
      .collect().size

    assert(results === 1)
  }

  test("DSL test for FAILFAST parsing mode") {
    val parser = new CsvParser()
      .withParseMode(ParseModes.FAIL_FAST_MODE)
      .withUseHeader(true)
      .withParserLib(parserLib)

    val exception = intercept[SparkException]{
      parser.csvFile(sqlContext, carsFile)
        .select("year")
        .collect()
    }

    assert(exception.getMessage.contains("Malformed line in FAILFAST mode: 2015,Chevy,Volt"))
  }

  test("DSL test roundtrip nulls") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "null-numbers.csv"
    val agesSchema = StructType(List(StructField("name", StringType, true),
                                     StructField("age", IntegerType, true)))

    val agesRows = Seq(Row("alice", 35), Row("bob", null), Row(null, 24))
    val agesRdd = sqlContext.sparkContext.parallelize(agesRows)
    val agesDf = sqlContext.createDataFrame(agesRdd, agesSchema)

    agesDf.saveAsCsvFile(copyFilePath, Map("header" -> "true", "nullValue" -> ""))

    val agesCopyOne = new CsvParser()
      .withSchema(agesSchema)
      .withUseHeader(true)
      .withTreatEmptyValuesAsNulls(true)
      .withParserLib(parserLib)
      .csvFile(sqlContext, copyFilePath)

    assert(agesCopyOne.count == agesRows.size)
    assert(agesCopyOne.collect.toSet == agesRows.toSet)

    val agesCopyTwo = new CsvParser()
      .withSchema(agesSchema)
      .withUseHeader(true)
      .withNullValue("")
      .withParserLib(parserLib)
      .csvFile(sqlContext, copyFilePath)

    assert(agesCopyTwo.count == agesRows.size)
    assert(agesCopyTwo.collect.toSet == agesRows.toSet)
  }

  test("DSL test for tokens more than the schema") {
    val results = sqlContext
      .csvFile(carsMalformedFile, parserLib = parserLib)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with alternative delimiter and quote") {
    val results = new CsvParser()
      .withDelimiter('|')
      .withQuoteChar('\'')
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvFile(sqlContext, carsAltFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with null quote character") {
    val results = new CsvParser()
      .withDelimiter(',')
      .withQuoteChar(null)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvFile(sqlContext, carsUnbalancedQuotesFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with alternative delimiter and quote using sparkContext.csvFile") {
    val results =
      sqlContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = '|',
        quote = '\'',
        parserLib = parserLib)
        .select("year")
        .collect()

    assert(results.size === numCars)
  }

  test("Expect parsing error with wrong delimiter setting using sparkContext.csvFile") {
    intercept[ org.apache.spark.sql.AnalysisException] {
      sqlContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = ',',
        quote = '\'',
        parserLib = parserLib)
        .select("year")
        .collect()
    }
  }

  test("Expect wrong parsing results with wrong quote setting using sparkContext.csvFile") {
    val results =
      sqlContext.csvFile(
        carsAltFile,
        useHeader = true,
        delimiter = '|',
        quote = '"',
        parserLib = parserLib)
        .select("year")
        .collect()

    assert(results.slice(0, numCars).toSeq.map(_(0).asInstanceOf[String]) ==
      Seq("'2012'", "1997", "2015"))
  }

  test("DDL test with alternative delimiter and quote") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|",
         |parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DSL test with empty file and known schema") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("column", StringType, false))))
      .withUseHeader(false)
      .withParserLib(parserLib)
      .csvFile(sqlContext, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val stringSchema = new StructType(
      Array(
        StructField("Name", StringType, true),
        StructField("Age", StringType, true),
        StructField("Height", StringType, true),
        StructField("Born", StringType, true)
      )
    )

    val results = new CsvParser()
      .withSchema(stringSchema)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .csvFile(sqlContext, ageFile)
      .count()

    assert(results === 4)
  }

  test("DSL test with poorly formatted file and known schema") {
    val strictSchema = new StructType(
      Array(
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("Height", DoubleType, true),
        StructField("Born", TimestampType, true)
      )
    )

    val results = new CsvParser()
      .withSchema(strictSchema)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .csvFile(sqlContext, ageFile)
      .count()

    assert(results === 1)
  }

  test("DDL test with empty file") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$emptyFile", header "false", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$carsFile", header "true", parserLib "$parserLib", nullValue "-")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT makeName FROM carsTable").collect().length === numCars)
    assert(sqlContext.sql("SELECT avg(yearMade) FROM carsTable where grp = '' group by grp")
      .collect().head(0) === 2004.5)
  }

  test("DSL column names test") {
    val cars = new CsvParser()
      .withUseHeader(false)
      .withParserLib(parserLib)
      .csvFile(sqlContext, carsFile)
    assert(cars.schema.fields(0).name == "C0")
    assert(cars.schema.fields(2).name == "C2")
  }

  test("SQL test insert overwrite") {
    // Create a temp directory for table that will be overwritten
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTableIO
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "true", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTableEmpty
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING com.databricks.spark.csv
           |OPTIONS (path "$tempEmptyDir", header "false", parserLib "$parserLib")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT * FROM carsTableIO").collect().size === numCars)
    assert(sqlContext.sql("SELECT * FROM carsTableEmpty").collect().isEmpty)

    sqlContext.sql(
      s"""
         |INSERT OVERWRITE TABLE carsTableEmpty
         |SELECT * FROM carsTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(sqlContext.sql("SELECT * FROM carsTableEmpty").collect().size == numCars)
  }

  test("DSL save") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true"))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with a quoteMode") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    val delimiter = ","
    var quote = "\""
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true",
      "quote" -> quote, "delimiter" -> delimiter, "quoteMode" -> "ALL"))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")
    for(file <- new File(copyFilePath + "/").listFiles) {
      if (!(file.getName.startsWith("_") || file.getName.startsWith("."))) {
        for(line <- Source.fromFile(file).getLines()) {
          for(column <- line.split(delimiter)) {
            assert(column.startsWith(quote))
            assert(column.endsWith(quote))
          }
        }
      }
    }

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with non numeric quoteMode") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib, inferSchema = true)
    val delimiter = ","
    var quote = "\""
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true",
      "quote" -> quote, "delimiter" -> delimiter, "quoteMode" -> "NON_NUMERIC"))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")
    for(file <- new File(copyFilePath + "/").listFiles) {
      if (!(file.getName.startsWith("_") || file.getName.startsWith("."))) {
        for((line, lineno) <- Source.fromFile(file).getLines().zipWithIndex) {
          val columns = line.split(delimiter)
          if (lineno == 0) {
            assert(columns(0).startsWith(quote))
            assert(columns(0).endsWith(quote))
            assert(columns(1).startsWith(quote))
            assert(columns(1).endsWith(quote))
          } else {
            assert(!columns(0).startsWith(quote))
            assert(!columns(0).endsWith(quote))
            assert(columns(1).startsWith(quote))
            assert(columns(1).endsWith(quote))
          }
        }
      }
    }

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with null quoteMode") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    val delimiter = ","
    var quote = "\""
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true",
      "quote" -> quote, "delimiter" -> delimiter, "quoteMode" -> null))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")
    for(file <- new File(copyFilePath + "/").listFiles) {
      if (!(file.getName.startsWith("_") || file.getName.startsWith("."))) {
        for(line <- Source.fromFile(file).getLines()) {
          for(column <- line.split(delimiter)) {
            assert(!column.startsWith(quote))
            assert(!column.endsWith(quote))
          }
        }
      }
    }

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL checking non null escapeChar is set before NONE quoteMode") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    val delimiter = ","
    var quote = "\""
    var escape = "\\"
    var quoteMode = "NONE"

    noException should be thrownBy {
      cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> quote,
        "delimiter" -> delimiter, "escape" -> escape, "quoteMode" -> quoteMode))
    }
  }

  test("DSL save with a compression codec") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true"), classOf[GzipCodec])

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("Scala API save with gzip compression codec") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    cars.save("com.databricks.spark.csv", SaveMode.Overwrite,
      Map("path" -> copyFilePath, "header" -> "true", "codec" -> classOf[GzipCodec].getName))
    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("Scala API save with gzip compression codec by shorten name") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    cars.save("com.databricks.spark.csv", SaveMode.Overwrite,
      Map("path" -> copyFilePath, "header" -> "true", "codec" -> "gZiP"))
    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.csvFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with quoting") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile, parserLib = parserLib)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "\""))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/", parserLib = parserLib)

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with alternate quoting") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.csv"

    val cars = sqlContext.csvFile(carsFile)
    cars.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "!"))

    val carsCopy = sqlContext.csvFile(copyFilePath + "/", quote = '!', parserLib = parserLib)

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with quoting, escaped quote") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "escape-copy.csv"

    val escape = sqlContext.csvFile(escapeFile, escape = '|', quote = '"')
    escape.saveAsCsvFile(copyFilePath, Map("header" -> "true", "quote" -> "\""))

    val escapeCopy = sqlContext.csvFile(copyFilePath + "/", parserLib = parserLib)

    assert(escapeCopy.count == escape.count)
    assert(escapeCopy.collect.map(_.toString).toSet == escape.collect.map(_.toString).toSet)
    assert(escapeCopy.head().getString(0) == "\"thing")
  }

  test("DSL test schema inferred correctly") {
    val results = sqlContext
      .csvFile(carsFile, parserLib = parserLib, inferSchema = true)

    val boolResults = sqlContext
      .csvFile(boolFile, parserLib = parserLib, inferSchema = true)

    assert(results.schema == StructType(List(
      StructField("year", IntegerType, nullable = true),
      StructField("make", StringType, nullable = true),
      StructField("model", StringType ,nullable = true),
      StructField("comment", StringType, nullable = true),
      StructField("blank", StringType, nullable = true))
    ))

    assert(boolResults.schema == StructType(List(
      StructField("bool", BooleanType, nullable = true))
    ))

    assert(results.collect().size === numCars)

    assert(boolResults.collect().size === 3)
  }

  test("DSL test inferred schema passed through") {
    val dataFrame = sqlContext
      .csvFile(carsFile, parserLib = parserLib, inferSchema = true)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.first.getString(0) === "No comment")
    assert(results.first.getInt(1) === 2012)
  }

  test("DDL test with inferred schema") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsFile", header "true", parserLib "$parserLib", inferSchema "true")
      """.stripMargin.replaceAll("\n", " "))

    val results = sqlContext.sql("select year from carsTable where make = 'Ford'")

    assert(results.first().getInt(0) === 1997)
  }

  test("DSL test nullable fields") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("name", StringType, false),
                                  StructField("age", IntegerType, true))))
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvFile(sqlContext, nullNumbersFile)
      .collect()

    assert(results.head.toSeq === Seq("alice", 35))
    assert(results(1).toSeq === Seq("bob", null))
    assert(results(2).toSeq === Seq("", 24))
  }

  test("DSL test nullable fields with user defined null value of \"null\"") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("name", StringType, false),
                                  StructField("age", IntegerType, true))))
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withNullValue("null")
      .csvFile(sqlContext, nullNullNumbersFile)
      .collect()

    assert(results.head.toSeq === Seq("alice", 35))
    assert(results(1).toSeq === Seq("bob", null))
    assert(results(2).toSeq === Seq("null", 24))
  }

  test("DSL test nullable fields with user defined null value of \"\\N\"") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("name", StringType, false),
                                  StructField("age", IntegerType, true))))
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withNullValue("\\N")
      .csvFile(sqlContext, nullSlashNNumbersFile)
      .collect()

    assert(results.head.toSeq === Seq("alice", 35))
    assert(results(1).toSeq === Seq("bob", null))
    assert(results(2).toSeq === Seq("\\N", 24))
  }

  test("Commented lines in CSV data") {
    val results: Array[Row] = new CsvParser()
      .withDelimiter(',')
      .withComment('~')
      .withParserLib(parserLib)
      .csvFile(sqlContext, commentsFile)
      .collect()

    val expected =
      Seq(Seq("1", "2", "3", "4", "5.01", "2015-08-20 15:57:00"),
          Seq("6", "7", "8", "9", "0", "2015-08-21 16:58:01"),
          Seq("1", "2", "3", "4", "5", "2015-08-23 18:00:42"))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("Inferring schema") {
    val results: Array[Row] = new CsvParser()
      .withDelimiter(',')
      .withComment('~')
      .withParserLib(parserLib)
      .withInferSchema(true)
      .csvFile(sqlContext, commentsFile)
      .collect()

    val expected =
      Seq(Seq(1, 2, 3, 4, 5.01D, Timestamp.valueOf("2015-08-20 15:57:00")),
          Seq(6, 7, 8, 9, 0, Timestamp.valueOf("2015-08-21 16:58:01")),
          Seq(1, 2, 3, 4, 5, Timestamp.valueOf("2015-08-23 18:00:42")))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("Inferring timestamp types via custom date format") {
    val results = new CsvParser()
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withDateFormat("dd/MM/yyyy hh:mm")
      .withNullValue("?")
      .withInferSchema(true)
      .csvFile(sqlContext, datesFile)
      .select("date")
      .collect()

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm")
    val expected =
      Seq(Seq(new Timestamp(dateFormatter.parse("26/08/2015 18:00").getTime)),
        Seq(new Timestamp(dateFormatter.parse("27/10/2014 18:30").getTime)),
        Seq(null),
        Seq(new Timestamp(dateFormatter.parse("28/01/2016 20:00").getTime)))
    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("Load date types via custom date format") {
    val customSchema = new StructType(Array(StructField("date", DateType, true)))
    val results = new CsvParser()
      .withSchema(customSchema)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withDateFormat("dd/MM/yyyy hh:mm")
      .withNullValue("?")
      .csvFile(sqlContext, datesFile)
      .select("date")
      .collect()

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm")
    val expected = Seq(
      new Date(dateFormatter.parse("26/08/2015 18:00").getTime),
      new Date(dateFormatter.parse("27/10/2014 18:30").getTime),
      null,
      new Date(dateFormatter.parse("28/01/2016 20:00").getTime))
    val dates = results.toSeq.map(_.toSeq.head)
    expected.zip(dates).foreach {
      case (null, date) => assert(date == null)
      case (expectedDate, date) =>
        // As it truncates the hours, minutes and etc., we only check
        // if the dates (days, months and years) are the same via `toString()`.
        assert(expectedDate.toString === date.toString)
    }
  }

  test("Setting comment to null disables comment support") {
    val results: Array[Row] = new CsvParser()
      .withDelimiter(',')
      .withComment(null)
      .withParserLib(parserLib)
      .csvFile(sqlContext, disableCommentsFile)
      .collect()

    val expected =
      Seq(
        Seq("#1", "2", "3"),
        Seq("4", "5", "6"))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("DSL allows for setting maxColsPerChar and expect error") {
    val parser = new CsvParser()
      .withDelimiter(',')
      .withUseHeader(true)
      .withParseMode(ParseModes.FAIL_FAST_MODE)
      .withMaxCharsPerCol(5000)

    val exception = intercept[SparkException]{
      parser.csvFile(sqlContext, longColsFile)
        .select("text")
        .collect()
    }

    assert(exception.getMessage.contains("Malformed line in FAILFAST mode: 2,Bacon ipsum dolor amet dolore"))
  }

  test("DSL allows for setting maxColsPerChar and succeeds") {
    val parser = new CsvParser()
      .withDelimiter(',')
      .withUseHeader(true)
      .withMaxCharsPerCol(15000)

    val res = parser.csvFile(sqlContext, longColsFile)
      .collect()

    assert(res.size === 3)
    assert(res(0).toSeq === Seq("1", "bacon is yummy"))
    assert(res(1).getAs[String](0) === "2")
    assert(res(1).getAs[String](1).startsWith("Bacon ipsum dolor amet dolore"))
    assert(res(2).toSeq === Seq("3", "pork is the best"))
  }

  test("DSL load csv from rdd") {
    val csvRdd = sqlContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))
    val df = new CsvParser()
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvRdd(sqlContext, csvRdd)
      .collect()

    assert(df(0).toSeq === Seq("20", "1.8"))
    assert(df(1).toSeq === Seq("16", "1.7"))
  }

  test("Inserting into csvRdd should throw exception"){
    val csvRdd = sqlContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))
    val sampleData = sqlContext.sparkContext.parallelize(Seq("age,height", "20,1.8", "16,1.7"))

    val df = new CsvParser()
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvRdd(sqlContext, csvRdd)
    val sampleDf = new CsvParser()
      .withUseHeader(true)
      .withParserLib(parserLib)
      .csvRdd(sqlContext, sampleData)

    df.registerTempTable("csvRdd")
    sampleDf.registerTempTable("sampleDf")

    val exception = intercept[java.io.IOException] {
      sqlContext.sql("INSERT OVERWRITE TABLE csvRdd select * from sampleDf")
    }
    assert(exception.getMessage.contains("Cannot INSERT into table with no path defined"))
  }

  test("DSL tsv test") {
    val results = sqlContext
      .tsvFile(carsTsvFile, parserLib = parserLib)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("Type/Schema inference works as expected for the simple sparse dataset.") {
    val df = new CsvParser()
      .withUseHeader(true)
      .withInferSchema(true)
      .csvFile(sqlContext, simpleDatasetFile)

    assert(
      df.schema.fields.map(_.dataType).deep ==
      Array(IntegerType, IntegerType, IntegerType, IntegerType).deep)
  }

  test("DSL test for DROPMALFORMED type inference without malformed lines") {
    val ages = new CsvParser()
      .withUseHeader(true)
      .withInferSchema(true)
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .withParserLib(parserLib)
      .csvFile(sqlContext, ageFileAlternative)

    assert(ages.count === numAges)
    assert(ages.schema.fields(0).dataType === StringType)
    assert(ages.schema.fields(1).dataType === DoubleType)
    assert(ages.schema.fields(2).dataType === IntegerType)
    assert(ages.schema.fields(3).dataType === TimestampType)
  }

  test("DSL test for DROPMALFORMED type inference with malformed lines") {
    val ages = new CsvParser()
      .withUseHeader(true)
      .withInferSchema(true)
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .withParserLib(parserLib)
      .csvFile(sqlContext, ageFileAlternativeMalformed)

    assert(ages.count === numAges - numMalformedAges)
    assert(ages.schema.fields(0).dataType === StringType)
    assert(ages.schema.fields(1).dataType === DoubleType)
    assert(ages.schema.fields(2).dataType === IntegerType)
    assert(ages.schema.fields(3).dataType === TimestampType)
  }

  test("DSL test for PERMISSIVE type inference with malformed lines") {
    val ages = new CsvParser()
      .withUseHeader(true)
      .withInferSchema(true)
      .withParseMode(ParseModes.PERMISSIVE_MODE)
      .withParserLib(parserLib)
      .csvFile(sqlContext, ageFileAlternativeMalformed)

    assert(ages.count === numAges)
    assert(ages.schema.fields(0).dataType === StringType)
    assert(ages.schema.fields(1).dataType === StringType)
    assert(ages.schema.fields(2).dataType === DoubleType)
    assert(ages.schema.fields(3).dataType === StringType)
  }
}

class CsvSuite extends AbstractCsvSuite {
  override def parserLib: String = "COMMONS"
}

class CsvFastSuite extends AbstractCsvSuite {
  override def parserLib: String = "UNIVOCITY"
}
