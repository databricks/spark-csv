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

import org.apache.spark.sql.test._
import org.apache.spark.sql.catalyst.types._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class CsvSuite extends FunSuite {
  val carsFile = "src/test/resources/cars.csv"
  val carsAltFile = "src/test/resources/cars-alternative.csv"
  val emptyFile = "src/test/resources/empty.csv"

  test("dsl test") {
    val results = TestSQLContext
      .csvFile(carsFile)
      .select('year)
      .collect()

    assert(results.size === 2)
  }

  test("sql test") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE carsTable
        |USING com.databricks.spark.csv
        |OPTIONS (path "$carsFile", header "true")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === 2)
  }

  test("dsl test with alternative delimiter and quote") {
    val results = new CsvParser()
      .withDelimiter('|')
      .withQuoteChar('\'')
      .csvFile(TestSQLContext, carsAltFile)
      .select('year)
      .collect()

    assert(results.size === 2)
  }

  test("sql test with alternative delimiter and quote") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.csv
         |OPTIONS (path "$carsAltFile", header "true", quote "'", delimiter "|")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT year FROM carsTable").collect().size === 2)
  }


  test("dsl test with empty file and known schema") {
    val results = new CsvParser()
      .withSchema(StructType(List(StructField("column", StringType, false))))
      .csvFile(TestSQLContext, emptyFile)
      .count()

    assert(results === 0)
  }

  //TODO(hossein): When relation Schema PR gets merged add a sql test for empty file
}
