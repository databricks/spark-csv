package com.databricks.spark.csv.util

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class InferSchemaSuite extends FunSuite {

  test("String fields types are inferred correctly from null types"){
    assert(InferSchema.inferField(NullType, "") == NullType)
    assert(InferSchema.inferField(NullType, null) == NullType)
    assert(InferSchema.inferField(NullType, "100000000000") == LongType)
    assert(InferSchema.inferField(NullType, "60") == IntegerType)
    assert(InferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(InferSchema.inferField(NullType, "test") == StringType)
  }

  test("String fields types are inferred correctly from other types"){
    assert(InferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(InferSchema.inferField(LongType, "test") == StringType)
    assert(InferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(InferSchema.inferField(DoubleType, null) == DoubleType)
    assert(InferSchema.inferField(DoubleType, "test") == StringType)
  }

  test("Type arrays are merged to highest common type"){
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
