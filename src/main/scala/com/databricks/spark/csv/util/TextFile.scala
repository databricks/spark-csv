package com.databricks.spark.csv.util

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[csv] object TextFile {
  val DEFAULT_CHARSET = StandardCharsets.UTF_8

  def withCharset(context: SparkContext, location: String, charset: String): RDD[String] = {
    if (Charset.forName(charset) == DEFAULT_CHARSET) {
      context.textFile(location)
    } else {
      // can't pass a Charset object here cause its not serializable
      // TODO: maybe use mapPartitions instead?
      context.hadoopFile[LongWritable, Text, TextInputFormat](location).map(
        pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)
      )
    }
  }
}
