# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table Updates
# MAGIC 
# MAGIC We have processed data from the Bronze table to the Silver table.
# MAGIC 
# MAGIC We now need to do some updates to ensure high data quality in the Silver
# MAGIC table. Because batch loading has no mechanism for checkpointing, we will
# MAGIC need a way to load _only the new records_ from the Bronze table.
# MAGIC 
# MAGIC We also need to deal with the quarantined records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Update the `read_batch_bronze` function to read only new records
# MAGIC 1. Fix the bad quarantined records from the Bronze table
# MAGIC 1. Write the repaired records to the Silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Raw to Bronze Pipeline

# COMMAND ----------

rawDF = read_batch_raw(rawPath)
transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Purge Raw File Path
# MAGIC 
# MAGIC Manually purge the raw files that have already been loaded.

# COMMAND ----------

rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_Ingestdate"
)

rawToBronzeWriter.save(bronzePath)

#Step3 Register the Bronze Table in the Metastore
spark.sql(
    """
DROP TABLE IF EXISTS movies_df_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE movies_df_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------



# COMMAND ----------

bronzeDF = read_batch_bronze(spark)
transformedBronzeDF = transform_bronze(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Bronze to Silver Pipeline
# MAGIC 
# MAGIC 
# MAGIC In the previous notebook, to ingest only the new data we ran
# MAGIC 
# MAGIC ```
# MAGIC bronzeDF = (
# MAGIC   spark.read
# MAGIC   .table("health_tracker_classic_bronze")
# MAGIC   .filter("status = 'new'")
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC **Exercise**
# MAGIC 
# MAGIC Update the function `read_batch_bronze` in the
# MAGIC `includes/main/python/operations` file so that it reads only the new
# MAGIC files in the Bronze table.

# COMMAND ----------

# MAGIC %md
# MAGIC ♨️ After updating the `read_batch_bronze` function, re-source the
# MAGIC `includes/main/python/operations` file to include your updates by running the cell below.

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

bronzeDF = read_batch_bronze(spark)
transformedBronzeDF = transform_bronze(bronzeDF)

(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(transformedBronzeDF)

genre_df_silver = generate_genre_dataframes(transformedBronzeDF)

junction_moviegenre_df_silver = generate_Juntion_MovieGenre_dataframes(transformedBronzeDF)

OriginalLanguage_df_silver = generate_originalLanguage_dataframes(transformedBronzeDF)

# COMMAND ----------

# Register
spark.sql(
    """
DROP TABLE IF EXISTS movies_df_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_df_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

#Genres
spark.sql(
    """
DROP TABLE IF EXISTS genres_df_silver
"""
)

spark.sql(
    f"""
CREATE TABLE genres_df_silver
USING DELTA
LOCATION "{silverGenrePath}"
"""
)

#Junction MovieGenres
spark.sql(
    """
DROP TABLE IF EXISTS movies_genres_df_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_genres_df_silver
USING DELTA
LOCATION "{silverJuncMovieGenrePath}"
"""
)

#OriginalLanguage
spark.sql(
    """
DROP TABLE IF EXISTS OriLanguage_df_silver
"""
)

spark.sql(
    f"""
CREATE TABLE OriLanguage_df_silver
USING DELTA
LOCATION "{silverOriginalLanguagePath}"
"""
)

# COMMAND ----------

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"quarantined"`.

# COMMAND ----------

# TODO
bronzeQuarantinedDF = spark.read.table("movies_df_bronze").filter("Status = 'quarantined'")
bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF)

repairDF = bronzeQuarTransDF.withColumn('Runtime', abs(col('Runtime'))).drop('genres')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Quarantined Records
# MAGIC 
# MAGIC This applies the standard bronze table transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Batch Write the Repaired (formerly Quarantined) Records to the Silver Table
# MAGIC 
# MAGIC After loading, this will also update the status of the quarantined records
# MAGIC to `loaded`.

# COMMAND ----------

bronzeToSilverQuarWriter = batch_writer(
    dataframe=repairDF, partition_column="p_ReleaseYear", exclude_columns=["Movies"]
)

bronzeToSilverQuarWriter.save(silverPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Update Status

# COMMAND ----------

update_bronze_table_status(spark, bronzePath, repairDF, "loaded")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
