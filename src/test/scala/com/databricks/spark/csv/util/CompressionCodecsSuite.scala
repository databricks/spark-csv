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
    assert(CompressionCodecs.getCodecClass(classOf[GzipCodec].getName) == classOf[GzipCodec])
    assert(CompressionCodecs.getCodecClass(classOf[SnappyCodec].getName) == classOf[SnappyCodec])
    assert(CompressionCodecs.getCodecClass(classOf[Lz4Codec].getName) == classOf[Lz4Codec])
    assert(CompressionCodecs.getCodecClass(classOf[BZip2Codec].getName) == classOf[BZip2Codec])
  }

  test("Get classes of compression codecs with short names") {
    assert(CompressionCodecs.getCodecClass("GzIp") == classOf[GzipCodec])
    assert(CompressionCodecs.getCodecClass("Snappy") == classOf[SnappyCodec])
    assert(CompressionCodecs.getCodecClass("lz4") == classOf[Lz4Codec])
    assert(CompressionCodecs.getCodecClass("bZip2") == classOf[BZip2Codec])
  }
}
