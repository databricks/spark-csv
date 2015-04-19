package com.databricks.spark.csv.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.Matchers

// Because this suite tests reflection, the test only works in SBT if the config uses forking.
// There is no workaround for Eclipse.
// See https://issues.apache.org/jira/browse/SPARK-5281.
class CsvToRDDSuite extends FunSuite with Matchers {
  import CsvToRDDSuite._
  import TestSQLContext._

  val carsFile = "src/test/resources/cars-with-typed-columns.csv"
  val carsFileTsv = "src/test/resources/cars-with-typed-columns.tsv"
  val carsFileWithoutHeaders = "src/test/resources/cars-with-typed-columns-without-headers.csv"

  test("DSL for RDD with DROPMALFORMED parsing mode") {
    val rdd = TestSQLContext.csvFileToRDD[Car](carsFile)
    rdd.collect() should contain theSameElementsAs Seq(
      Car(2012, "Tesla", "S", "No comment", 1, 350000.00),
      Car(1997, "Ford", "E350", "Go get one now they are going fast", 3, 25000.00))
  }

  test("DSL for RDD with DROPMALFORMED parsing mode, TSV") {
    val rdd = TestSQLContext.tsvFileToRDD[Car](carsFileTsv)
    rdd.collect() should contain theSameElementsAs Seq(
      Car(2012, "Tesla", "S", "No comment", 1, 350000.00),
      Car(1997, "Ford", "E350", "Go get one now they are going fast", 3, 25000.00))
  }

  test("DSL for RDD with DROPMALFORMED parsing mode, without headers") {
    val rdd = TestSQLContext.csvFileToRDD[Car](carsFileWithoutHeaders, useHeader = false)
    rdd.collect() should contain theSameElementsAs Seq(
      Car(2012, "Tesla", "S", "No comment", 1, 350000.00),
      Car(1997, "Ford", "E350", "Go get one now they are going fast", 3, 25000.00))
  }

  test("DSL for RDD with FAILFAST parsing mode") {
    intercept[org.apache.spark.SparkException] {
      val rdd = TestSQLContext.csvFileToRDD[Car](carsFile, mode = "FAILFAST")
      println(rdd.collect())
    }
  }

  test("DSL for RDD with PERMISSIVE parsing mode") {
    intercept[IllegalArgumentException] {
      TestSQLContext.csvFileToRDD[Car](carsFile, mode = "PERMISSIVE")
    }
  }

  test("DSL for RDD with invalid type argument") {
    intercept[IllegalArgumentException] {
      TestSQLContext.csvFileToRDD[CarWithNonPrimitive](carsFile)
    }
  }
}

object CsvToRDDSuite {
  case class Car(year: Int, make: String, model: String, comment: String, stocked: Int, price: Double)
  case class CarWithNonPrimitive(year: Int, makeAndModel: MakeAndModel, comment: String, stocked: Int, price: Double)
  case class MakeAndModel(make: String, model: String)
}
