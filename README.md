# Spark SQL CSV Library

A library for parsing and querying CSV data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/databricks/spark-csv)

## Requirements

This library requires Spark 1.3+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.databricks
artifactId: spark-csv_2.10
version: 1.0.0
```
The spark-csv assembly jar file can also be added to a Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-csv-assembly-1.0.0.jar
```

## Features
These examples use a CSV file available for download [here](https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv):

```
$ wget https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv
```

### Scala API

You can use the library by loading the implicits from `com.databricks.spark.csv._`.

```
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import com.databricks.spark.csv._

val cars = sqlContext.csvFile("cars.csv")
```

### SQL API
CSV data can be queried in pure SQL by registering the data as a (temporary) table.

```sql
CREATE TABLE cars
USING com.databricks.spark.csv
OPTIONS (path "cars.csv", header "true")
```

You can also specify column names and types in DDL.
```sql
CREATE TABLE cars (yearMade double, carMake string, carModel string, comments string, blank string)
USING com.databricks.spark.csv
OPTIONS (path "cars.csv", header "true")
```

### Java API
CSV files can be read using functions in JavaCsvParser.

```java
import com.databricks.spark.csv.JavaCsvParser;

DataFrame cars = (new JavaCsvParser()).withUseHeader(true).csvFile(sqlContext, "cars.csv");
```

### Saving as CSV
You can save your DataFrame using `saveAsCsvFile` function. The function allows you to specify the delimiter and whether we should generate a header row for the table (each header has name `C$i` where `$i` is column index). For example:
```myDataFrame.saveAsCsvFile("/mydir", Map("delimiter" -> "|", "header" -> "true"))```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt assembly` from the project root.
