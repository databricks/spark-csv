package com.databricks.spark.csv;

import java.io.File;
import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.test.TestSQLContext$;
import static org.apache.spark.sql.functions.*;


public class JavaCsvSuite {
  private transient SQLContext sqlContext;

  String carsFile = "src/test/resources/cars.csv";

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    sqlContext = TestSQLContext$.MODULE$;
  }

  @After
  public void tearDown() {
    sqlContext = null;
  }

  @Test
  public void testCsvParser() {
    DataFrame df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    int result = df.select("model").collect().length;
    Assert.assertEquals(result, 2);
  }

  @Test
  public void testLoad() {
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("header", "true");
    options.put("path", carsFile);

    DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
    int result = df.select("year").collect().length;
    Assert.assertEquals(result, 2);
  }
}