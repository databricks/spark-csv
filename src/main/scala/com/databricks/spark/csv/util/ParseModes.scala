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

private[csv] object ParseModes {

  val PERMISSIVE_MODE = "PERMISSIVE"
  val SUPER_PERMISSIVE_MODE = "SUPERPERMISSIVE"
  val DROP_MALFORMED_MODE = "DROPMALFORMED"
  val FAIL_FAST_MODE = "FAILFAST"

  val DEFAULT = PERMISSIVE_MODE

  def isValidMode(mode: String): Boolean = {
    mode.toUpperCase match {
      case PERMISSIVE_MODE | SUPER_PERMISSIVE_MODE | DROP_MALFORMED_MODE | FAIL_FAST_MODE => true
      case _ => false
    }
  }

  def isDropMalformedMode(mode: String): Boolean = mode.toUpperCase == DROP_MALFORMED_MODE
  def isFailFastMode(mode: String): Boolean = mode.toUpperCase == FAIL_FAST_MODE
  def isSuperPermissiveMode(mode: String): Boolean = mode.toUpperCase == SUPER_PERMISSIVE_MODE
  def isPermissiveMode(mode: String): Boolean = if (isValidMode(mode))  {
    mode.toUpperCase == PERMISSIVE_MODE
  } else {
    true // We default to permissive is the mode string is not valid
  }
}
