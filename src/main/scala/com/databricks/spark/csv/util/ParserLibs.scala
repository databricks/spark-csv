// scalastyle:off
/*
 * Copyright 2015 Ayasdi Inc
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
// scalastyle:on
package com.databricks.spark.csv.util

private[csv] object ParserLibs {
  val OLD = "COMMONS"
  val NEW = "UNIVOCITY"

  val DEFAULT = OLD

  def isValidLib(lib: String): Boolean = {
    lib.toUpperCase match {
      case OLD | NEW => true
      case _ => false
    }
  }

  def isCommonsLib(lib: String): Boolean = if (isValidLib(lib)) {
    lib.toUpperCase == OLD
  } else {
    true  // default
  }

  def isUnivocityLib(lib: String): Boolean = if (isValidLib(lib)) {
    lib.toUpperCase == NEW
  } else {
    false  // not the default
  }

}
