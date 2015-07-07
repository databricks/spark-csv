package com.databricks.spark.csv.util

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class InferSchemaSuite extends FunSuite {

  test("String fields types are inferred correctly"){

    assert(InferSchema.inferField("") == NullType)
    assert(InferSchema.inferField(null) == NullType)
    assert(InferSchema.inferField("100000000000") == LongType)
    assert(InferSchema.inferField("60") == IntegerType)
    assert(InferSchema.inferField("3.5") == DoubleType)
    assert(InferSchema.inferField("test") == StringType)

  }

}
