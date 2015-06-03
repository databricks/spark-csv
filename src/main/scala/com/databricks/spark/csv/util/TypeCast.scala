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

import java.sql.{Timestamp, Date}

import org.apache.spark.sql.types._

/**
 * Utility functions for type casting
 */
object TypeCast {

  /**
   * Casts given string datum to specified type.
   * Currently we do not support complex types (ArrayType, MapType, StructType) and DecimalType.
   *
   * @param datum string value
   * @param castType SparkSQL type
   */
  private[csv] def castTo(datum: String, castType: DataType): Any = {
    castType match {
      case ByteType => datum.toByte
      case ShortType => datum.toShort
      case IntegerType => datum.toInt
      case LongType => datum.toLong
      case FloatType => datum.toFloat
      case DoubleType => datum.toDouble
      case BooleanType => datum.toBoolean
      // TODO(hossein): would be good to support other common timestamp formats
      case TimestampType => Timestamp.valueOf(datum)
      // TODO(hossein): would be good to support other common date formats
      case DateType => Date.valueOf(datum)
      case StringType => datum
      case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
    }
  }
}
