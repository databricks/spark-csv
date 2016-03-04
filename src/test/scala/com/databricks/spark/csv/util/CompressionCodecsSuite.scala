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
import org.scalatest.FunSuite

class CompressionCodecsSuite extends FunSuite {

  /**
   * Note that Lz4 codec was added from Hadoop 2.x. So, some tests might fail with
   * class-not-found exception when Hadoop version is lower.
   */
  test("Get classes of compression codecs") {
    assert(CompressionCodecs.getCodecClass(classOf[GzipCodec].getName).get == classOf[GzipCodec])
    assert(
      CompressionCodecs.getCodecClass(classOf[DeflateCodec].getName).get == classOf[DeflateCodec])
    assert(
      CompressionCodecs.getCodecClass(classOf[SnappyCodec].getName).get == classOf[SnappyCodec])
    assert(CompressionCodecs.getCodecClass(classOf[Lz4Codec].getName).get == classOf[Lz4Codec])
    assert(CompressionCodecs.getCodecClass(classOf[BZip2Codec].getName).get == classOf[BZip2Codec])
  }

  test("Get classes of compression codecs with short names") {
    assert(CompressionCodecs.getCodecClass("noNe").get == null)
    assert(CompressionCodecs.getCodecClass("Uncompressed").get == null)
    assert(CompressionCodecs.getCodecClass("deflate").get == classOf[DeflateCodec])
    assert(CompressionCodecs.getCodecClass("GzIp").get == classOf[GzipCodec])
    assert(CompressionCodecs.getCodecClass("Snappy").get == classOf[SnappyCodec])
    assert(CompressionCodecs.getCodecClass("lz4").get == classOf[Lz4Codec])
    assert(CompressionCodecs.getCodecClass("bZip2").get == classOf[BZip2Codec])
  }
}
