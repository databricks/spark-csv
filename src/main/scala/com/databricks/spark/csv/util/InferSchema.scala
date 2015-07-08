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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.util.control.Exception._

private[csv] object InferSchema {

  /**
   * Similar to the JSON schema inference. {@link org.apache.spark.sql.json.InferSchema}
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(tokenRdd: RDD[Array[String]], header: Array[String]): StructType = {

    val rootTypes = tokenRdd.aggregate(Array[DataType]())(inferRowType, mergeRowTypes)

    val stuctFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      StructField(thisHeader, rootType, nullable = true)
    }

    StructType(stuctFields)
  }

  private[csv] def inferRowType(rowSoFar: Array[DataType], next: Array[String]) = {
    mergeRowTypes(rowSoFar, next.map(inferField))
  }

  private[csv] def mergeRowTypes(first: Array[DataType], second: Array[DataType]) = {
    first.zipAll(second, NullType, NullType).map { case ((a, b)) =>
      val tpe = findTightestCommonType(a, b).getOrElse(StringType)
      tpe match {
        case _: NullType => StringType
        case other => other
      }
    }
  }

  private[csv] def inferField(field: String): DataType = {

    if (field == null || field.isEmpty) {
      NullType
    } else if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else if ((allCatch opt field.toDouble).isDefined) {
      DoubleType
    } else {
      StringType
    }

  }

  /**
   * Copied from internal Spark api
   * {@link org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion}
   */
  private val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType.Unlimited)

  /**
   * Copied from internal Spark api
   * {@link org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion}
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    // Fixed-precision decimals can up-cast into unlimited
    case (DecimalType.Unlimited, _: DecimalType) => Some(DecimalType.Unlimited)
    case (_: DecimalType, DecimalType.Unlimited) => Some(DecimalType.Unlimited)

    case _ => None
  }
}
