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

import org.scalatest.FunSuite

import org.apache.spark.sql.types.{FloatType, DoubleType, DecimalType}

class TypeCastSuite extends FunSuite {

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType(None)

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      assert(TypeCast.castTo(strVal, decimalType) === new BigDecimal(decimalVal.toString))
    }
  }

  test("Can parse special") {
    val strValues = Seq("NaN", "Infinity", "-Infinity")
    val doubleChecks: Seq[Double => Boolean] = Seq(x => x.isNaN, x => x.isPosInfinity, x => x.isNegInfinity)
    val floatChecks: Seq[Float => Boolean] = Seq(x => x.isNaN, x => x.isPosInfinity, x => x.isNegInfinity)

    strValues.zip(doubleChecks).foreach { case (strVal, checker) =>
      assert(checker(TypeCast.castTo(strVal, DoubleType).asInstanceOf[Double]))
    }
    strValues.zip(floatChecks).foreach { case (strVal, checker) =>
      assert(checker(TypeCast.castTo(strVal, FloatType).asInstanceOf[Float]))
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
}
