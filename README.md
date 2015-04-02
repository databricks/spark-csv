# Spark SQL CSV Library

A library for parsing and querying CSV data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/databricks/spark-csv)

## Requirements

This library requires Spark 1.3+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.databricks
artifactId: spark-csv_2.11
version: 1.0.2
```

## Using with Apache Spark
This package can be added to  Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages com.databricks:spark-csv_2.10:1.0.2
```

## Features
This package allows reading CSV files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* path: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* header: when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is false.
* delimiter: by default lines are delimited using ',', but delimiter can be set to any character
* quote: by default the quote character is '"', but can be set to any character. Delimiters inside quotes are ignored
* mode: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
  * PERMISSIVE: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
  * DROPMALFORMED: drops lines which have fewer or more tokens than expected
  * FAILFAST: aborts with a RuntimeException if encounters any malformed line

The package also support saving simple (non-nested) DataFrame. When saving you can specify the delimiter and whether we should generate a header row for the table. See following examples for more details.

These examples use a CSV file available for download [here](https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv):

```
$ wget https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv
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

### Scala API
The recommended way to load CSV data is using the load/save functions in SQLContext.

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "cars.csv", "header" -> "true"))
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```

You can also use the implicits from `com.databricks.spark.csv._`.

```scala
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv._

val sqlContext = new SQLContext(sc)

val cars = sqlContext.csvFile("cars.csv")
cars.select("year", "model").saveAsCsvFile("newcars.tsv")
```

### Java API
Similar to Scala, we recommend load/save functions in SQLContext.

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);

HashMap<String, String> options = new HashMap<String, String>();
options.put("header", "true");
options.put("path", "cars.csv");

DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv");
```
See documentations of <a href="https://spark.apache.org/docs/1.3.0/api/java/org/apache/spark/sql/SQLContext.html#load(java.lang.String)">load</a> and <a href="https://spark.apache.org/docs/1.3.0/api/java/org/apache/spark/sql/DataFrame.html#save(java.lang.String)">save</a> for more details.

In Java (as well as Scala) CSV files can be read using functions in CsvParser.

```java
import com.databricks.spark.csv.CsvParser;
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

DataFrame cars = (new CsvParser()).withUseHeader(true).csvFile(sqlContext, "cars.csv");
```

### Python API
In Python you can read and save CSV files using load/save functions.

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "cars.csv")
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
