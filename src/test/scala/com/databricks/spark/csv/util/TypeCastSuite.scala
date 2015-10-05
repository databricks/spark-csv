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
package com.databricks.spark.csv.util

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.scalatest.FunSuite

import org.apache.spark.sql.types._

class TypeCastSuite extends FunSuite {

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType(None)

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      assert(TypeCast.castTo(strVal, decimalType) === new BigDecimal(decimalVal.toString))
    }
  }

  test("Can parse escaped characters") {
    assert(TypeCast.toChar("""\t""") === '\t')
    assert(TypeCast.toChar("""\r""") === '\r')
    assert(TypeCast.toChar("""\b""") === '\b')
    assert(TypeCast.toChar("""\f""") === '\f')
    assert(TypeCast.toChar("""\"""") === '\"')
    assert(TypeCast.toChar("""\'""") === '\'')
    assert(TypeCast.toChar("""\u0000""") === '\u0000')
  }

  test("Does not accept delimiter larger than one character") {
    val exception = intercept[IllegalArgumentException]{
      TypeCast.toChar("ab")
    }
    assert(exception.getMessage.contains("cannot be more than one character"))
  }

  test("Throws exception for unsupported escaped characters") {
    val exception = intercept[IllegalArgumentException]{
      TypeCast.toChar("""\1""")
    }
    assert(exception.getMessage.contains("Unsupported special character for delimiter"))
  }

  test("Nullable types are handled") {
    assert(TypeCast.castTo("", IntegerType, nullable = true) == null)
  }

  test("String type should always return the same as the input") {
    assert(TypeCast.castTo("", StringType, nullable = true) == "")
    assert(TypeCast.castTo("", StringType, nullable = false) == "")
  }

  test("Throws exception for empty string with non null type") {
    val exception = intercept[NumberFormatException]{
      TypeCast.castTo("", IntegerType, nullable = false)
    }
    assert(exception.getMessage.contains("For input string: \"\""))
  }

  test("Types are cast correctly") {
    assert(TypeCast.castTo("10", ByteType) == 10)
    assert(TypeCast.castTo("10", ShortType) == 10)
    assert(TypeCast.castTo("10", IntegerType) == 10)
    assert(TypeCast.castTo("10", LongType) == 10)
    assert(TypeCast.castTo("1.00", FloatType) == 1.0)
    assert(TypeCast.castTo("1,00", DoubleType) == 1.0)
    assert(TypeCast.castTo("1.00", DoubleType) == 1.0)
    assert(TypeCast.castTo("1,00", DoubleType) == 1.0)
    assert(TypeCast.castTo("true", BooleanType) == true)
    val timestamp = "2015-01-01 00:00:00"
    assert(TypeCast.castTo(timestamp, TimestampType) == Timestamp.valueOf(timestamp))
    assert(TypeCast.castTo("2015-01-01", DateType) == Date.valueOf("2015-01-01"))
  }
}
