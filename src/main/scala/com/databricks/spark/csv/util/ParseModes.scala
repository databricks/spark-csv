package com.databricks.spark.csv.util

private[csv] object ParseMode {
  val PERMISSIVE_MODE = "PERMISSIVE"
  val DROP_MALFORMED_MODE = "DROPMALFORMED"
  val FAIL_FAST_MODE = "FAILFAST"

  def isValidMode(mode: String): Boolean = {
    mode.toUpperCase match {
      case PERMISSIVE_MODE | DROP_MALFORMED_MODE | FAIL_FAST_MODE => true
      case _ => false
    }
  }

  def isPermissiveMode(mode: String) = mode.toUpperCase == PERMISSIVE_MODE
  def isDropMalformedMode(mode: String) = mode.toUpperCase == DROP_MALFORMED_MODE
  def isFailFastMode(mode: String) = mode.toUpperCase == FAIL_FAST_MODE
}