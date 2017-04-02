// MAGIC %md # Step 0: Import your data
// MAGIC 
// MAGIC Use the movielens dataset provided with Spark.

val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import spark.implicits._
import spark.sql

// COMMAND ----------

// If you are under $SPARK_HOME, you can just use "data/mllib/sample_movielens_data.txt" as textFile source
val rdd = sc.textFile("/Users/wt/Desktop/spark-2.1.0-bin-hadoop2.7/data/mllib/sample_movielens_data.txt").map { line =>

val Array(user, item, rating) = line.split("::")
  (user, item, rating)
}

val df = sqlContext.createDataFrame(rdd).toDF("user", "item", "rating")

df.show
