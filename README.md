# Spark CSV Library

A library for parsing and querying CSV data with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://travis-ci.org/databricks/spark-csv.svg?branch=master)](https://travis-ci.org/databricks/spark-csv)
[![codecov.io](http://codecov.io/github/databricks/spark-csv/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-csv?branch=master)

## Requirements

This library requires Spark 1.3+

## Linking
You can link against this library in your program at the following coordiates:

### Scala 2.10
```
groupId: com.databricks
artifactId: spark-csv_2.10
version: 1.1.0
```
### Scala 2.11
```
groupId: com.databricks
artifactId: spark-csv_2.11
version: 1.1.0
```


## Using with Spark shell
This package can be added to  Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages com.databricks:spark-csv_2.10:1.1.0
```

## Features
This package allows reading CSV files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `header`: when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is false.
* `delimiter`: by default lines are delimited using ',', but delimiter can be set to any character
* `quote`: by default the quote character is '"', but can be set to any character. Delimiters inside quotes are ignored
* `mode`: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
  * `PERMISSIVE`: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
  * `DROPMALFORMED`: drops lines which have fewer or more tokens than expected
  * `FAILFAST`: aborts with a RuntimeException if encounters any malformed line
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names
* `inferSchema`: automatically infers column types. It requires one extra pass over the data and is false by default
* `comment`: skip lines beginning with this character. Default is `"#"`. Disable comments by setting this to `null`.

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
Spark 1.4+:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("cars.csv")
df.select("year", "model").write.format("com.databricks.spark.csv").save("newcars.csv")
```

Spark 1.3:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "cars.csv", "header" -> "true"))
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```


### Java API
Spark 1.4+:
```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load("cars.csv");
df.select("year", "model").write().format("com.databricks.spark.csv").save("newcars.csv");
```

Spark 1.3:
```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);

HashMap<String, String> options = new HashMap<String, String>();
options.put("header", "true");
options.put("path", "cars.csv");

DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv");
```

### Python API
Spark 1.4+:
```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('cars.csv')
df.select('year', 'model').write.format('com.databricks.spark.csv').save('newcars.csv')
```

Spark 1.3:
```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="com.databricks.spark.csv", header="true", path = "cars.csv")
df.select("year", "model").save("newcars.csv", "com.databricks.spark.csv")
```

### R API
Spark 1.4+:
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.1.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)
df <- read.df(sqlContext, "cars.csv", source = "com.databricks.spark.csv")

write.df(df, "newcars.csv", "com.databricks.spark.csv", "overwrite")
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
