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

import scala.util.control.Exception._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress._

private[csv] object CompressionCodecs {
  private val shortCompressionCodecNames: Map[String, String] = {
    val codecMap = collection.mutable.Map.empty[String, String]
    codecMap += "none" -> null
    codecMap += "uncompressed" -> null
    allCatch toTry(codecMap += "deflate" -> classOf[DeflateCodec].getName)
    allCatch toTry(codecMap += "bzip2" -> classOf[BZip2Codec].getName)
    allCatch toTry(codecMap += "gzip" -> classOf[GzipCodec].getName)
    allCatch toTry(codecMap += "lz4" -> classOf[Lz4Codec].getName)
    allCatch toTry(codecMap += "snappy" -> classOf[SnappyCodec].getName)
    codecMap.toMap
  }

  /**
   * Return the codec class of the given name.
   */
  def getCodecClass: String => Option[Class[_ <: CompressionCodec]] = {
    case null =>
      None
    case codec =>
      val codecName = shortCompressionCodecNames.getOrElse(codec.toLowerCase, codec)
      try {
        val clazz = if (codecName != null) {
          // scalastyle:off classforname
          Class.forName(codecName).asInstanceOf[Class[CompressionCodec]]
          // scalastyle:on classforname
        } else {
          null
        }
        Some(clazz)
      } catch {
        case e: ClassNotFoundException =>
          throw new IllegalArgumentException(s"Codec [$codecName] is not " +
            s"available. Known codecs are ${shortCompressionCodecNames.keys.mkString(", ")}.")
      }
  }

  /**
   * Disable compression codec.
   */
  def disableCompressConfiguration(hadoopConf: Configuration): Unit = {
    // This infers the option `compression` is set to `uncompressed` or `none`.
    hadoopConf.set("mapred.output.compress", "false")
    hadoopConf.set("mapred.compress.map.output", "false")
    hadoopConf.set("mapreduce.output.fileoutputformat.compress", "false")
    hadoopConf.set("mapreduce.map.output.compress", "false")
  }
}
