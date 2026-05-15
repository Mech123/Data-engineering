# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion with Auto Loader
# MAGIC
# MAGIC This notebook ingests prepared hydraulic sensor JSONL files from a landing zone
# MAGIC into Bronze Delta tables using Databricks Auto Loader.
# MAGIC
# MAGIC ## Source
# MAGIC - sensor_raw_cycles JSONL files
# MAGIC - profile_raw JSONL files
# MAGIC
# MAGIC ## Target Bronze Tables
# MAGIC - sensorops_dev.bronze.sensor_raw_cycles
# MAGIC - sensorops_dev.bronze.profile_raw

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC In a real project, these values should come from job parameters, environment variables,
# MAGIC or Databricks Asset Bundle variables.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "sensorops_dev")
dbutils.widgets.text("bronze_schema", "bronze")

dbutils.widgets.text(
    "sensor_raw_source_path",
    "dbfs:/mnt/sensorops/landing/bronze_source/sensor_raw_cycles",
)

dbutils.widgets.text(
    "profile_raw_source_path",
    "dbfs:/mnt/sensorops/landing/bronze_source/profile_raw",
)

dbutils.widgets.text(
    "checkpoint_base_path",
    "dbfs:/mnt/sensorops/checkpoints/bronze",
)

dbutils.widgets.text(
    "schema_base_path",
    "dbfs:/mnt/sensorops/schemas/bronze",
)

catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")

sensor_raw_source_path = dbutils.widgets.get("sensor_raw_source_path")
profile_raw_source_path = dbutils.widgets.get("profile_raw_source_path")

checkpoint_base_path = dbutils.widgets.get("checkpoint_base_path")
schema_base_path = dbutils.widgets.get("schema_base_path")

sensor_raw_target_table = f"{catalog_name}.{bronze_schema}.sensor_raw_cycles"
profile_raw_target_table = f"{catalog_name}.{bronze_schema}.profile_raw"

print(f"Sensor source path: {sensor_raw_source_path}")
print(f"Profile source path: {profile_raw_source_path}")
print(f"Sensor target table: {sensor_raw_target_table}")
print(f"Profile target table: {profile_raw_target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create catalog and schema if needed
# MAGIC
# MAGIC In production, catalog and schema creation is often handled separately by platform/admin scripts.
# MAGIC For this project, we keep this here so the notebook can run end-to-end in a dev workspace.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define expected schemas
# MAGIC
# MAGIC We use explicit schemas instead of relying only on inference.
# MAGIC This gives stronger control and is closer to production practice.

# COMMAND ----------

sensor_raw_schema = StructType(
    [
        StructField("cycle_id", LongType(), False),
        StructField("asset_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("physical_quantity", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("sampling_rate_hz", IntegerType(), True),
        StructField("expected_reading_count", IntegerType(), True),
        StructField("actual_reading_count", IntegerType(), True),
        StructField("readings", ArrayType(DoubleType()), True),
        StructField("source_file_name", StringType(), True),
        StructField("source_file_path", StringType(), True),
        StructField("structure_status", StringType(), True),
    ]
)

profile_raw_schema = StructType(
    [
        StructField("cycle_id", LongType(), False),
        StructField("asset_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("cooler_condition_pct", IntegerType(), True),
        StructField("valve_condition_pct", IntegerType(), True),
        StructField("internal_pump_leakage", IntegerType(), True),
        StructField("hydraulic_accumulator_bar", IntegerType(), True),
        StructField("stable_flag", IntegerType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("source_file_path", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest sensor raw cycles with Auto Loader
# MAGIC
# MAGIC Auto Loader uses `cloudFiles` to incrementally process new files from cloud/object storage.
# MAGIC The checkpoint tracks which files have already been processed.

# COMMAND ----------

sensor_raw_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{schema_base_path}/sensor_raw_cycles")
    .schema(sensor_raw_schema)
    .load(sensor_raw_source_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
    .withColumn("source_input_file", F.input_file_name())
)

sensor_raw_query = (
    sensor_raw_stream_df.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_base_path}/sensor_raw_cycles")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(sensor_raw_target_table)
)

sensor_raw_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest profile raw labels with Auto Loader

# COMMAND ----------

profile_raw_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{schema_base_path}/profile_raw")
    .schema(profile_raw_schema)
    .load(profile_raw_source_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
    .withColumn("source_input_file", F.input_file_name())
)

profile_raw_query = (
    profile_raw_stream_df.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_base_path}/profile_raw")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(profile_raw_target_table)
)

profile_raw_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quick validation queries

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS sensor_raw_count FROM {sensor_raw_target_table}"))
display(spark.sql(f"SELECT COUNT(*) AS profile_raw_count FROM {profile_raw_target_table}"))

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            sensor_id,
            physical_quantity,
            COUNT(*) AS cycle_count,
            MIN(actual_reading_count) AS min_reading_count,
            MAX(actual_reading_count) AS max_reading_count
        FROM {sensor_raw_target_table}
        GROUP BY sensor_id, physical_quantity
        ORDER BY sensor_id
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            cooler_condition_pct,
            valve_condition_pct,
            internal_pump_leakage,
            hydraulic_accumulator_bar,
            stable_flag,
            COUNT(*) AS cycle_count
        FROM {profile_raw_target_table}
        GROUP BY
            cooler_condition_pct,
            valve_condition_pct,
            internal_pump_leakage,
            hydraulic_accumulator_bar,
            stable_flag
        ORDER BY cycle_count DESC
        """
    )
)
