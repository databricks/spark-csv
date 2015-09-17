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

import java.sql.Timestamp

import scala.util.control.Exception._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

private[csv] object InferSchema {

  /**
   * Similar to the JSON schema inference. [[org.apache.spark.sql.json.InferSchema]]
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(tokenRdd: RDD[Array[String]], header: Array[String]): StructType = {

    val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
    val rootTypes: Array[DataType] = tokenRdd.aggregate(startType)(inferRowType, mergeRowTypes)

    val stuctFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      StructField(thisHeader, rootType, nullable = true)
    }

    StructType(stuctFields)
  }

  private def inferRowType(rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      rowSoFar(i) = inferField(rowSoFar(i), next(i))
      i+=1
    }
    rowSoFar
  }

  private[csv] def mergeRowTypes(
      first: Array[DataType],
      second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case ((a, b)) =>
      val tpe = findTightestCommonType(a, b).getOrElse(StringType)
      tpe match {
        case _: NullType => StringType
        case other => other
      }
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  private[csv] def inferField(typeSoFar: DataType, field: String): DataType = {
    if (field == null || field.isEmpty) {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseInteger(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case DoubleType => tryParseDouble(field)
        case TimestampType => tryParseTimestamp(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }


  private def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
    IntegerType
  } else {
    tryParseLong(field)
  }

  private def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
    LongType
  } else {
    tryParseDouble(field)
  }

  private def tryParseDouble(field: String): DataType = {
    if ((allCatch opt field.toDouble).isDefined) {
      DoubleType
    } else {
      tryParseTimestamp(field)
    }
  }

  def tryParseTimestamp(field: String): DataType = {
    if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
      TimestampType
    } else {
      stringType()
    }
  }

  // Defining a function to return the StringType constant is necessary in order to work around
  // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
  // see issue #128 for more details.
  private def stringType(): DataType = {
    StringType
  }

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.Unlimited)

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}
