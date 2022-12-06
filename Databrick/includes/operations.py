# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,abs
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.types import _parse_datatype_string
from pyspark.sql.window import Window

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        ) 
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )
     

# COMMAND ----------

def batch_writer_lookuptable(
    dataframe: DataFrame,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
    )

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    schema_json = 'movie ARRAY'
    movies = spark.read.option('multiline', 'true').schema(schema_json).json(rawPath)
    return movies.withColumn('Movies', explode(col("movie")))

# COMMAND ----------

def read_batch_bronze(spark) -> DataFrame:
    return spark.read.table("movies_df_bronze").filter("Status = 'New'")

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        "Movies",
        current_timestamp().alias("Ingesttime"),
        lit("New").alias("Status"),
        current_timestamp().cast("date").alias("p_Ingestdate"),
    )

# COMMAND ----------

def transform_bronze(bronzeDF: DataFrame, quarantine: bool = False) -> DataFrame:
    json_schema = StructType(fields=[
        StructField('Id', LongType(), True),
        StructField('Title', StringType(), True),     
        StructField('Overview', StringType(), True),
        StructField('OriginalLanguage', StringType(), True),
        StructField('Price', DoubleType(), True),
        StructField('ReleaseDate', TimestampType(), True),
        StructField('Budget', DoubleType(), True),
        StructField('Revenue', DoubleType(), True),
        StructField('RunTime', LongType(), True),
        StructField('Tagline', StringType(), True),
        StructField('CreatedBy', StringType(), True),
        StructField('CreatedDate', TimestampType(), True),
        StructField('UpdatedBy', StringType(), True),
        StructField('UpdatedDate', TimestampType(), True),
        StructField('ImdbUrl', StringType(), True),
        StructField('TmdbUrl', StringType(), True),
        StructField('PosterUrl', StringType(), True),
        StructField('BackdropUrl', StringType(), True),
        StructField('genres', ArrayType(
            StructType([
                StructField('id', IntegerType(), True),
                StructField('name', StringType(), True)
            ])
        ))
    ])
    
    StructType(fields=[
  
        StructField('TmdbUrl', StringType(), True),
        StructField(
            'genres', ArrayType(
                StructType([
                    StructField('id', IntegerType(), True),
                    StructField('name', StringType(), True)
                ])
            )
        )
    ])

    bronzeAugmentedDF = bronzeDF.withColumn("nested_json", from_json(col("Movies"), json_schema))
    
    movies_df_silver = bronzeAugmentedDF.select("Movies", "nested_json.*")
    return movies_df_silver.select(
        'Movies',
        'Id',
        'Title',
        'Overview',
        'OriginalLanguage',
        'Price',
        'ReleaseDate',
        year(col('ReleaseDate').cast('date')).alias('p_ReleaseYear'),
        'Budget',
        'Revenue',
        'RunTime',
        'Tagline',
        'genres',
        'CreatedBy',
        'CreatedDate',
        'UpdatedBy',
        'UpdatedDate',
        'ImdbUrl',
        'TmdbUrl',
        'PosterUrl',
        'BackdropUrl'
    ).dropDuplicates()


# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.drop('genres').filter("RunTime >= 0"),
        dataframe.drop('genres').filter("RunTime < 0"),
    )

# COMMAND ----------

def generate_genre_dataframes(dataframe: DataFrame):
    genres = dataframe.select('Id', 'genres')
    genre_exploded = genres.withColumn('genre_json', explode('genres')).drop("genres").dropDuplicates()

    return genre_exploded.select(
        col('genre_json.id').alias('Genre_Id'),
        col('genre_json.name').alias('Genre_Name')
    ).dropDuplicates(["Genre_Id"])

# COMMAND ----------

def generate_originalLanguage_dataframes(dataframe: DataFrame):
    originalLanguage = dataframe.select("OriginalLanguage").dropDuplicates()

    return originalLanguage.select(
        col('OriginalLanguage').alias('Language_Code'),
        lit('English').alias('Language_Name')
    )

# COMMAND ----------

def generate_Juntion_MovieGenre_dataframes(dataframe: DataFrame):
    genres = dataframe.select('Id', 'genres')
    genre_exploded = genres.withColumn('genre_json', explode('genres')).drop("genres").dropDuplicates()
  
    return genre_exploded.select(
        col('Id').alias('Movie_Id'),
        col('genre_json.id').alias('Genre_Id')
    )

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("Status", lit(status))

    update_match = "bronze.Movies = dataframe.Movies"
    update = {"Status": "dataframe.Status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True

# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF
