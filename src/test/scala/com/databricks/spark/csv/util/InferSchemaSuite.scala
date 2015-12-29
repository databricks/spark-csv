package com.databricks.spark.csv.util

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class InferSchemaSuite extends FunSuite {

  test("String fields types are inferred correctly from null types") {
    assert(InferSchema.inferField(NullType, "") == NullType)
    assert(InferSchema.inferField(NullType, null) == NullType)
    assert(InferSchema.inferField(NullType, "100000000000") == LongType)
    assert(InferSchema.inferField(NullType, "60") == IntegerType)
    assert(InferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(InferSchema.inferField(NullType, "test") == StringType)
    assert(InferSchema.inferField(NullType, "2015-08-20 15:57:00") == TimestampType)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    assert(InferSchema.inferField(NullType, "null", "null") == NullType)
    assert(InferSchema.inferField(StringType, "null", "null") == StringType)
    assert(InferSchema.inferField(LongType, "null", "null") == LongType)
    assert(InferSchema.inferField(IntegerType, "\\N", "\\N") == IntegerType)
    assert(InferSchema.inferField(DoubleType, "\\N", "\\N") == DoubleType)
    assert(InferSchema.inferField(TimestampType, "\\N", "\\N") == TimestampType)
  }

  test("String fields types are inferred correctly from other types") {
    assert(InferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(InferSchema.inferField(LongType, "test") == StringType)
    assert(InferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(InferSchema.inferField(DoubleType, null) == DoubleType)
    assert(InferSchema.inferField(DoubleType, "test") == StringType)
    assert(InferSchema.inferField(LongType, "2015-08-20 14:57:00") == TimestampType)
    assert(InferSchema.inferField(DoubleType, "2015-08-20 15:57:00") == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    assert(InferSchema.inferField(IntegerType, "2015-08-20 14") == StringType)
    assert(InferSchema.inferField(DoubleType, "2015-08-20 14:10") == StringType)
    assert(InferSchema.inferField(LongType, "2015-08 14:49:00") == StringType)
  }

  test("Type arrays are merged to highest common type") {
    assert(
      InferSchema.mergeRowTypes(Array(StringType),
      Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      InferSchema.mergeRowTypes(Array(IntegerType),
      Array(LongType)).deep == Array(LongType).deep)
    assert(
      InferSchema.mergeRowTypes(Array(DoubleType),
      Array(LongType)).deep == Array(DoubleType).deep)
  }

}
