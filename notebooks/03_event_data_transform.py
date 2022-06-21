# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# Load the event data
df = spark.readStream\
        .format("csv") \
        .option("delimiter", ",")\
        .schema(event_data_schema) \
        .option("header","true")\
        .option("multiline", "true") \
        .option('badRecordsPath', bad_records_path)\
        .load(event_source_path)

display(df)

# COMMAND ----------

# Validate the event data
df.createOrReplaceTempView(event_data_tv)
valid_records, invalid_records = validate_records()
display(invalid_records)

# COMMAND ----------

# Save the invalid data into the delta table
def save_invalid_data(df):
    columns = []
    for sf in df.schema.fields:
        columns.append(sf.name)
    
    df = df.withColumn('record', to_json(struct(col("*"))))
    df = df.drop(*columns) \
            .withColumn("rule", lit(event_valid_sql)) \
            .withColumn("reason", lit("No relevant master data found.")) \
            .withColumn("status", lit("retryable")) \
            .withColumn("retry_attempt", lit(0)) \
            .withColumn("retry_attempt_limit", lit(3)) \
            .withColumn("created_at", current_timestamp())

    df.writeStream \
      .format("delta")\
      .outputMode("append") \
      .option("path",error_table_path)\
      .option("checkpointLocation", error_table_ckpt_path) \
      .toTable(error_table_name)

# COMMAND ----------

save_invalid_data(invalid_records)

# COMMAND ----------

display(valid_records)

# COMMAND ----------

# Transform the valid records
valid_records.createOrReplaceTempView(valid_event_data_tv)
transformed_records = spark.sql(event_transform_sql)

# COMMAND ----------

# Save the transformed data into the delta table
transformed_records.writeStream \
    .format("delta")\
    .outputMode("append")\
    .option("path",event_table_path)\
    .option("checkpointLocation", event_table_ckpt_path) \
    .toTable(event_table_name)

# COMMAND ----------

df = spark.sql(f"select * from {event_table_name}")
display(df)

# COMMAND ----------

df = spark.sql(f"select * from {error_table_name}")
display(df)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/error_handling/mock_events/file_0.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/bad_records/bad_records/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/bad_records/bad_records/part-00000-932aa5e9-6d73-4d96-b682-991f72ac5bf7
