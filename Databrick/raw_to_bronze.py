# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw to Bronze Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest Raw Data
# MAGIC 2. Augment the data with Ingestion Metadata
# MAGIC 3. Batch write the augmented data to a Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

raw_path = 'dbfs:/FileStore/tables'
bronzePath = 'dbfs:/tables'

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data
# MAGIC 
# MAGIC Next, we will read files from the source directory and write each line as a string to the Bronze table.
# MAGIC 
# MAGIC 🤠 You should do this as a batch load using `spark.read`
# MAGIC 
# MAGIC Read in using the format, `"text"`, and using the provided schema.

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_json
movie_raw = spark.read.json(path = raw_path, multiLine = True)
movie_raw = movie_raw.select("movie", explode("movie"))
movie_raw = movie_raw.drop(col("movie")).toDF('movie')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data
# MAGIC 
# MAGIC 🤓 Each row here is a raw string in JSON format, as would be passed by a stream server like Kafka.

# COMMAND ----------

display(movie_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata
# MAGIC 
# MAGIC As part of the ingestion process, we record metadata for the ingestion.
# MAGIC 
# MAGIC **EXERCISE:** Add metadata to the incoming raw data. You should add the following columns:
# MAGIC 
# MAGIC - data source (`datasource`), use `"files.training.databricks.com"`
# MAGIC - ingestion time (`ingesttime`)
# MAGIC - status (`status`), use `"new"`
# MAGIC - ingestion date (`ingestdate`)

# COMMAND ----------

# TODO
from pyspark.sql.functions import current_timestamp, lit

movie_raw = movie_raw.select(
    "movie",
    lit("antra.sep.databatch.movieshop").alias("datasource"),
    lit("new").alias("status"),
    current_timestamp().alias("ingesttime"),
    current_timestamp().cast("date").alias("ingestdate")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table
# MAGIC 
# MAGIC Finally, we write to the Bronze Table.
# MAGIC 
# MAGIC Make sure to write in the correct order (`"datasource"`, `"ingesttime"`, `"value"`, `"status"`, `"p_ingestdate"`).
# MAGIC 
# MAGIC Make sure to use following options:
# MAGIC 
# MAGIC - the format `"delta"`
# MAGIC - using the append mode
# MAGIC - partition by `p_ingestdate`

# COMMAND ----------

# TODO
from pyspark.sql.functions import col

(
movie_raw.select("datasource", "ingesttime", "movie", "status", col("ingestdate").alias("p_ingestdate"))
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore
# MAGIC 
# MAGIC The table should be named `health_tracker_classic_bronze`.

# COMMAND ----------

# TODO

spark.sql("""
DROP TABLE IF EXISTS movies_bronze
""")

# Create the bronze table
spark.sql(f"""
CREATE TABLE movies_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table
# MAGIC 
# MAGIC Run this query to display the contents of the Classic Bronze Table

# COMMAND ----------

movies_bronze = spark.read.load(path = bronzePath)
display(movies_bronze)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
