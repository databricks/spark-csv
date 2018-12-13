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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

abstract class AbstractCsvWriteSuite extends FunSuite with BeforeAndAfterAll {

  val datesFile = "src/test/resources/dates.csv"
  val tempEmptyDir = "target/test/empty/"
  val weoFile = "src/test/resources/weo_apr_2017_all.csv"

  def parserLib: String

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "CsvWriteSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("Save with default date format") {
    mkTempDir()
    val dates = readDatesFromFile()
    val copyFilePath = tempEmptyDir + "dates-copy.csv"
    val retDataFile = tempEmptyDir + "dates-result.csv"

    // Write dataframe this way prior in spark 1.3 and before
    // (later versions use dates.write.format)
    dates.saveAsCsvFile(copyFilePath, Map("header" -> "false"))

    FileUtil.fullyDelete(new File(retDataFile))
    merge(copyFilePath, retDataFile)

    val actualContents = readFile(retDataFile)

    assert("2015-08-26 18:00:00.0\n2014-10-27 18:30:00.0\nnull\n2016-01-28 20:00:00.0"
      === actualContents)
  }

  test("Save with custom date format") {
    mkTempDir()
    val dates = readDatesFromFile()

    val copyFilePath = tempEmptyDir + "dates-copy.csv"
    val retDataFile = tempEmptyDir + "dates-result.csv"

    // Write dataframe this way prior in spark 1.3 and before
    dates.saveAsCsvFile(copyFilePath,
      Map("header" -> "false", "dateFormat" -> "MM/dd/yyyy HH:mm:ss"))

    FileUtil.fullyDelete(new File(retDataFile))
    merge(copyFilePath, retDataFile)

    val actualContents = readFile(retDataFile)

    // note that dates have been written with custom format
    assert("08/26/2015 18:00:00\n10/27/2014 18:30:00\nnull\n01/28/2016 20:00:00" === actualContents)
  }

  test("Save multiple csv files with headers") {
    mkTempDir()
    val weo = readWeoFromFile()
    weo.show()

    val copyFilePath = tempEmptyDir + "weo-copy.csv"

    weo.saveAsCsvFile(copyFilePath, Map("header" -> "true"))
    //
    //    FileUtil.fullyDelete(new File(retDataFile))
    //    merge(copyFilePath, retDataFile)
    //
    //    val actualContents = readFile(retDataFile)

    // note that dates have been written with custom format
  }

  // Create temp directory
  def mkTempDir(): Unit = {
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
  }

  def readDatesFromFile(): DataFrame = {
    val customSchema = new StructType(Array(StructField("date", TimestampType, true)))
    new CsvParser()
      .withSchema(customSchema)
      .withUseHeader(true)
      .withParserLib(parserLib)
      .withNullValue("?")
      .withDateFormat("dd/MM/yyyy HH:mm")
      .csvFile(sqlContext, datesFile)
      .select("date")
  }

  def readWeoFromFile(): DataFrame = {
    new CsvParser()
      .withUseHeader(true)
      .csvFile(sqlContext, weoFile)
      .select("WEO Country Code",
        "ISO",
        "WEO Subject Code",
        "Country",
        "Subject Descriptor",
        "Subject Notes",
        "Units",
        "Scale",
        "Country/Series-specific Notes",
        "1980",
        "1981",
        "1982",
        "1983",
        "1984",
        "1985",
        "1986",
        "1987",
        "1988",
        "1989",
        "1990",
        "1991",
        "1992",
        "1993",
        "1994",
        "1995",
        "1996",
        "1997",
        "1998",
        "1999",
        "2000",
        "2001",
        "2002",
        "2003",
        "2004",
        "2005",
        "2006",
        "2007",
        "2008",
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "Estimates Start After"
      )
  }

  def readFile(fname: String): String = {
    val source = Source.fromFile(fname)
    try source.getLines.mkString("\n") finally source.close()
  }

  private def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
}


class CsvWriteSuite extends AbstractCsvWriteSuite {
  override def parserLib: String = "COMMONS"
}

class CsvFastWriteSuite extends AbstractCsvWriteSuite {
  override def parserLib: String = "UNIVOCITY"
}
