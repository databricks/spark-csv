package com.databricks.spark.csv;

import java.io.File;
import java.util.HashMap;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.test.TestSQLContext$;

public class JavaCsvSuite {
  private transient SQLContext sqlContext;
  private int numCars = 3;

  String carsFile = "src/test/resources/cars.csv";

  private String tempDir = "target/test/csvData/";

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
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testSQL() {
      sqlContext.sql("CREATE TEMPORARY TABLE carsTable " +
                      "USING com.databricks.spark.csv " +
                      "OPTIONS (path \""+carsFile+"\", header \"true\"");
      DataFrame df = sqlContext.sql("SELECT * FROM carsTable");
      Assert.assertEquals(df.collect().length, 3);
  }

  @Test
  public void testLoad() {
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("header", "true");
    options.put("path", carsFile);

    DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
    int result = df.select("year").collect().length;
    Assert.assertEquals(result, numCars);
  }

  @Test
  public void testSave() {
    DataFrame df = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, carsFile);
    TestUtils.deleteRecursively(new File(tempDir));
    df.select("year", "model").save(tempDir, "com.databricks.spark.csv");

    DataFrame newDf = (new CsvParser()).csvFile(sqlContext, tempDir);
    int result = newDf.select("C1").collect().length;
    Assert.assertEquals(result, numCars);

  }
}
