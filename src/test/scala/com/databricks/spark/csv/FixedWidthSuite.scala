package com.databricks.spark.csv

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.specs2.mutable.Specification
import org.specs2.specification.After

trait FixedWidthSetup extends After {
  protected def fruit_resource(name: String = ""): String =
    s"src/test/resources/fruit_${name}_fixedwidth.txt"

  protected val fruitWidths = Array(3, 10, 5, 4)
  protected val fruitSize = 7
  protected val malformedFruitSize = 5
  protected val fruitFirstRow = Seq(56, "apple", "TRUE", 0.56)

  protected val fruitSchema = StructType(Seq(
    StructField("val", IntegerType),
    StructField("name", StringType),
    StructField("avail", StringType),
    StructField("cost", DoubleType)
  ))

  val sqlContext: SQLContext = new SQLContext(new SparkContext("local[2]", "FixedwidthSuite"))

  def after: Unit = sqlContext.sparkContext.stop()
}

class FixedWidthSpec extends Specification with FixedWidthSetup {

  protected def sanityChecks(resultSet: DataFrame) = {
    resultSet.show()
    resultSet.collect().length mustEqual fruitSize

    val head = resultSet.head()
    head.length mustEqual fruitWidths.length
    head.toSeq mustEqual fruitFirstRow
  }

  "FixedwidthParser" should {
    "Parse a basic fixed width file, successfully" in {
      val result = sqlContext.fixedWidthFile(fruit_resource(), fruitWidths, fruitSchema,
        useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with headers, and ignore them" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("w_headers"), fruitWidths,
        fruitSchema, useHeader = true)
      sanityChecks(result)
    }

    "Parse a fw file with overflowing lines, and ignore the overflow" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("overflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a fw file with underflowing lines, successfully " in {
      val result = sqlContext.fixedWidthFile(fruit_resource("underflow"), fruitWidths,
        fruitSchema, useHeader = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema and without inferring types, successfully" in {
      val result = sqlContext.fixedWidthFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = false)
      sanityChecks(result)
    }

    "Parse a basic fw file without schema, and infer the schema" in {
      val result = sqlContext.fixedWidthFile(fruit_resource(), fruitWidths,
        useHeader = false, inferSchema = true)
      sanityChecks(result)
    }

    "Parse a fw file with headers but without schema and without inferrence, succesfully" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("w_headers"), fruitWidths,
        useHeader = true, inferSchema = true)
      sanityChecks(result)
    }

    "Parse a fw file with comments, and ignore those lines" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("comments"), fruitWidths,
        useHeader = true, inferSchema = true, comment = '/')
      sanityChecks(result)
    }

    "Parse a malformed fw and schemaless file in PERMISSIVE mode, successfully" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("malformed"), fruitWidths,
        useHeader = false, mode = "PERMISSIVE")
      result.show()
      result.collect().length mustEqual fruitSize
    }

    "Parse a malformed and schemaless fw file in DROPMALFORMED mode, " +
      "successfully dropping bad lines" in {
      val result = sqlContext.fixedWidthFile(fruit_resource("malformed"), fruitWidths,
        useHeader = false, mode = "DROPMALFORMED")
      result.show()
     result.collect().length mustEqual malformedFruitSize
    }

    "FAIL to parse a malformed fw file with schema in FAILFAST mode" in {
      def fail: Array[Row] = {
        sqlContext.fixedWidthFile(fruit_resource("malformed"), fruitWidths,
          fruitSchema, useHeader = false, mode = "FAILFAST").collect()
      }
      fail must throwA[SparkException]
    }

    "FAIL to parse a fw file with the wrong format" in {
      def fail: Array[Row] = {
        sqlContext.fixedWidthFile(fruit_resource("wrong_schema"), fruitWidths,
          fruitSchema, useHeader = false).collect()
      }
      fail must throwA[SparkException]
    }
  }

}
