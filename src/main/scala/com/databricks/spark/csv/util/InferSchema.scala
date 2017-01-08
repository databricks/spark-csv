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
import java.text.SimpleDateFormat

import scala.util.control.Exception._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

private[csv] object InferSchema {

  /**
   * Similar to the JSON schema inference.
   * [[org.apache.spark.sql.execution.datasources.json.InferSchema]]
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(
      tokenRdd: RDD[Array[String]],
      header: Array[String],
      dropMalformed: Boolean = false,
      nullValue: String = "",
      dateFormatter: SimpleDateFormat = null): StructType = {
    val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
    val rootTypes: Array[DataType] = tokenRdd.aggregate(startType)(
      inferRowType(nullValue, header, dateFormatter, dropMalformed),
      mergeRowTypes)

    val structFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case z: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }

    StructType(structFields)
  }

  private def inferRowType(
      nullValue: String,
      header: Array[String],
      dateFormatter: SimpleDateFormat,
      dropMalformed: Boolean = false)
  (rowSoFar: Array[DataType], next: Array[String]): Array[DataType] = {
    var i = 0
    if (header.length != next.length && dropMalformed) {
      // Type inference should not be based on malformed lines in case of DROPMALFORMED parse mode
      rowSoFar
    } else {
      while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
        rowSoFar(i) = inferField(rowSoFar(i), next(i), nullValue, dateFormatter)
        i+=1
      }
      rowSoFar
    }
  }

  private[csv] def mergeRowTypes(
      first: Array[DataType],
      second: Array[DataType]): Array[DataType] = {
    first.zipAll(second, NullType, NullType).map { case ((a, b)) =>
      findTightestCommonType(a, b).getOrElse(NullType)
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  private[csv] def inferField(typeSoFar: DataType,
      field: String,
      nullValue: String = "",
      dateFormatter: SimpleDateFormat = null): DataType = {
    def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field)
    }

    def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDouble(field)
    }

    def tryParseDouble(field: String): DataType = {
      if ((allCatch opt field.toDouble).isDefined) {
        DoubleType
      } else {
        tryParseTimestamp(field)
      }
    }

    def tryParseTimestamp(field: String): DataType = {
      if (dateFormatter != null) {
        // This case infers a custom `dataFormat` is set.
        if ((allCatch opt dateFormatter.parse(field)).isDefined){
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      } else {
        // We keep this for backwords competibility.
        if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      }
    }

    def tryParseBoolean(field: String): DataType = {
      if ((allCatch opt field.toBoolean).isDefined) {
        BooleanType
      } else {
        stringType()
      }
    }

    // Defining a function to return the StringType constant is necessary in order to work around
    // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
    // see issue #128 for more details.
    def stringType(): DataType = {
      StringType
    }

    if (field == null || field.isEmpty || field == nullValue) {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseInteger(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case DoubleType => tryParseDouble(field)
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
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
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}
