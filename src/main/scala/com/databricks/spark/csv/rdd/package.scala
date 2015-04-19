package com.databricks.spark.csv

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

package object rdd {
  implicit class CsvContextRDD(sqlContext: SQLContext) {
    def csvFileToRDD[T: scala.reflect.runtime.universe.TypeTag : scala.reflect.ClassTag](
      filePath: String,
      useHeader: Boolean = true,
      delimiter: Char = ',',
      quote: Char = '"',
      escape: Char = '\\',
      mode: String = "DROPMALFORMED"): RDD[T] = {

      if (mode == util.ParseModes.PERMISSIVE_MODE)
        throw new IllegalArgumentException(s"permissive mode is invalid for this method")

      val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
      if (schema.exists { structField => !structField.dataType.isPrimitive })
        throw new IllegalArgumentException(s"type must be a case class with only primitive fields")

      val df = new CsvContext(sqlContext).csvFile(filePath, useHeader, delimiter, quote, escape, mode, Some(schema))
      df.mapPartitions[T] { iter =>
        val rowConverter = RowConverter[T]()
        iter.map { row => rowConverter.convert(row) }
      }
    }

    def tsvFileToRDD[T: scala.reflect.runtime.universe.TypeTag : scala.reflect.ClassTag](
      filePath: String,
      useHeader: Boolean = true,
      mode: String = "DROPMALFORMED"): RDD[T] = {
      csvFileToRDD[T](filePath, useHeader, delimiter = '\t', quote = '"', escape = '\\', mode)
    }
  }

  case class RowConverter[T]()(implicit ct: scala.reflect.ClassTag[T]) {
    // http://docs.scala-lang.org/overviews/reflection/environment-universes-mirrors.html#types-of-mirrors-their-use-cases--examples

    // For Scala 2.10, because we're initializing the runtime universe, this is not thread-safe.
    // http://docs.scala-lang.org/overviews/reflection/thread-safety.html
    val ru = scala.reflect.runtime.universe

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(ct.runtimeClass)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.toType.declaration(ru.nme.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)

    def convert(row: Row): T = {
      val args = row.toSeq
      require(constructorSymbol.paramss.head.size == args.size)
      constructorMirror.apply(args: _*).asInstanceOf[T]
    }
  }
}