# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

schema = StructType([
           StructField('id', IntegerType(), True),
           StructField('amount', IntegerType(), True),
           StructField('ts', TimestampType(), True)
         ])

df = spark.readStream\
        .format("csv") \
        .option("delimiter", ",")\
        .schema(schema) \
        .option("header","true")\
        .option("multiline", "true") \
        .option('badRecordsPath', bad_records_path)\
        .load(event_source_path)

display(df)

# COMMAND ----------

df.createOrReplaceTempView(event_temp_view)

def validate_records():
    valid_records = spark.sql(f'select * from {event_temp_view} e where e.id in (select m.id from {master_table_name} as m)')
    invalid_records = spark.sql(f'select * from {event_temp_view} e where e.id not in (select m.id from {master_table_name} as m)')
    return valid_records, invalid_records

valid_records, invalid_records = validate_records()
display(invalid_records)

# COMMAND ----------

def save_invalid_data(df):
    columns = []
    for sf in df.schema.fields:
        columns.append(sf.name)
    
    df = df.withColumn('record', to_json(struct(col("*"))))
    df = df.drop(*columns) \
            .withColumn("rule", lit("rule")) \
            .withColumn("reason", lit("reason")) \
            .withColumn("error_type", lit("retryable")) \
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

valid_records.writeStream.format("delta")\
    .outputMode("append")\
    .option("path",event_table_path)\
    .option("checkpointLocation", event_table_ckpt_path) \
    .toTable(event_table_name)
df = spark.sql(f"select * from {event_table_name}")
display(df)

# COMMAND ----------

df = spark.sql(f"select * from {error_table_name}")
display(df)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/mock_events/file_0.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/bad_records/bad_records/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/bad_records/bad_records/part-00000-932aa5e9-6d73-4d96-b682-991f72ac5bf7
