import org.apache.spark.{SparkConf, SparkContext}

object SparkExample extends App{

  var conf = new SparkConf()
  conf.setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  //Write
  var df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|").load("hdfs://localhost:9000/user/hyukjinkwon/lineitem.tbl")
  df.registerTempTable("lineitem")

  val result_df = sqlContext.sql(
    """
         SELECT *
         FROM lineitem
    """
  )
  result_df.printSchema()
  result_df.collect().foreach(consume(_))
  def consume (value: Any)= {}
}


//0.505
//0.287
//0.106

//0.489
//0.502
//0.536