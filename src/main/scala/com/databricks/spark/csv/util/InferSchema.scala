package com.databricks.spark.csv.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.types._

import scala.util.control.Exception._

private[csv] object InferSchema {

  /**
   * Similar to the JSON schema inference. {@link org.apache.spark.sql.json.InferSchema}
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(
             tokenRdd: RDD[Array[String]],
             header: Array[String]
             ): StructType = {

    val rootTypes: Array[DataType] = tokenRdd.map { tokens =>
      tokens.map(inferField)
    }.treeReduce {
      case (firstTypeArray, secondTypeArray) => firstTypeArray.zipAll(secondTypeArray, NullType, NullType).map { case ((a, b)) =>
        val tpe = HiveTypeCoercion.findTightestCommonType(a, b).getOrElse(StringType)
        tpe match {
          case _: NullType => StringType
          case other => other
        }
      }
    }

    val stuctFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      StructField(thisHeader, rootType, nullable = true)
    }

    StructType(stuctFields)
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

}
