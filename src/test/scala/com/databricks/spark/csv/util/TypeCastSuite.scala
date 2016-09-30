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
import java.text.SimpleDateFormat
import java.util.Locale

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
    assert(TypeCast.castTo("-", ByteType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", ShortType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", IntegerType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", LongType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", FloatType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", DoubleType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", BooleanType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", TimestampType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", DateType, nullable = true, nullValue = "-") == null)
    assert(TypeCast.castTo("-", StringType, nullable = true, nullValue = "-") == null)
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
    assert(TypeCast.castTo("1.00", DoubleType) == 1.0)
    assert(TypeCast.castTo("true", BooleanType) == true)
    val timestamp = "2015-01-01 00:00:00"
    assert(TypeCast.castTo(timestamp, TimestampType) == Timestamp.valueOf(timestamp))
    assert(TypeCast.castTo("2015-01-01", DateType) == Date.valueOf("2015-01-01"))

    val dateFormatter = new SimpleDateFormat("dd/MM/yyyy hh:mm")
    val customTimestamp = "31/01/2015 00:00"
    // `SimpleDateFormat.parse` returns `java.util.Date`. This needs to be converted
    // to `java.sql.Date`
    val expectedDate = new Date(dateFormatter.parse("31/01/2015 00:00").getTime)
    val expectedTimestamp = new Timestamp(expectedDate.getTime)
    assert(TypeCast.castTo(customTimestamp, TimestampType, dateFormatter = dateFormatter)
      == expectedTimestamp)
    assert(TypeCast.castTo(customTimestamp, DateType, dateFormatter = dateFormatter) ==
      expectedDate)
  }

  test("Float and Double Types are cast correctly with Locale") {
    val locale : Locale = new Locale("fr", "FR")
    Locale.setDefault(locale)
    assert(TypeCast.castTo("1,00", FloatType) == 1.0)
    assert(TypeCast.castTo("1,00", DoubleType) == 1.0)
  }

  test("Can handle mapping user specified nullValues") {
    assert(TypeCast.castTo("null", StringType, true, false, "null") == null)
    assert(TypeCast.castTo("\\N", ByteType, true, false, "\\N") == null)
    assert(TypeCast.castTo("", ShortType, true, false) == null)
    assert(TypeCast.castTo("null", StringType, true, true, "null") == null)
    assert(TypeCast.castTo("", StringType, true, false, "") == null)
    assert(TypeCast.castTo("", StringType, true, true, "") == null)
  }
}
