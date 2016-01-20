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

import org.apache.hadoop.io.compress._

private[csv] object CompressionCodecs {
  private val shortCompressionCodecNames = Map(
    "bzip2" -> classOf[BZip2Codec].getName,
    "gzip" -> classOf[GzipCodec].getName,
    "lz4" -> classOf[Lz4Codec].getName,
    "snappy" -> classOf[SnappyCodec].getName)

  /**
   * Return the full version of the given codec class.
   * If it is already a class name, just return it.
   */
  def getCodecClass: String => Class[_ <: CompressionCodec] = {
    case null => null
    case codec =>
      val codecName = shortCompressionCodecNames.getOrElse(codec.toLowerCase, codec)
      val codecClass = try {
        // scalastyle:off classforname
        Some(Class.forName(codecName).asInstanceOf[Class[CompressionCodec]])
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException => None
      }
      codecClass.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not " +
        s"available. Available codecs are ${shortCompressionCodecNames.keys.mkString(", ")}."))
  }
}
