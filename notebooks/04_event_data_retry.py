# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, error_table_path):
    raise Exception("Error table not found.")
    

# COMMAND ----------

schema = StructType([
        StructField("id", IntegerType()),
        StructField("amount", IntegerType()),
        StructField("ts", TimestampType())
    ])

event_columns = []
for sf in schema.fields:
    event_columns.append(sf.name)

df_all = spark.read.format("delta").load(error_table_path)
error_columns = []
for sf in df_all.schema.fields:
    error_columns.append(sf.name)

# COMMAND ----------

# Get retryable invalid records
df_retryable = df_all.where("status = 'retryable' and retry_attempt<retry_attempt_limit")
if df_retryable.count()==0:
    print(f"{error_table_path}: no retryable record.")
    dbutils.notebook.exit("event retry")
df_remain = df_all.subtract(df_retryable)
df_retryable = df_retryable.withColumn("json_record",from_json(col("record"),schema)) \
        .select("json_record.*","*")\
        .drop("json_record")
df_retryable.createOrReplaceTempView(event_temp_view)
df_retryable.show()

# COMMAND ----------

valid_records, invalid_records = validate_records()
display(invalid_records)

# COMMAND ----------

# Get valid data
valid_data = valid_records.drop(*error_columns)
valid_data.show()
valid_data_count = valid_data.count()
valid_data.createOrReplaceTempView(valid_event_data_tv)
valid_records = valid_records.drop(*event_columns)\
                            .withColumn("retry_attempt",col("retry_attempt")+1)\
                            .withColumn("rule",lit(event_valid_sql))\
                            .withColumn("status",lit("successful"))
display(valid_records)

# COMMAND ----------

valid_data.write.format("delta")\
                .mode("append")\
                .option("path",event_table_path)\
                .saveAsTable(event_table_name)

# COMMAND ----------

df = spark.sql(f"select * from {event_table_name}")
display(df)

# COMMAND ----------

# Update retry_attempt of invalid records
invalid_records = invalid_records.withColumn("retry_attempt",col("retry_attempt")+1)\
                            .withColumn("status", when(col("retry_attempt")>=col("retry_attempt_limit"),"failed").otherwise("retryable"))\
                            .withColumn("rule",lit(event_valid_sql))\
                            .drop(*event_columns)

# Overwrite the error table
df_remain = df_remain.union(valid_records).union(invalid_records)
df_remain.show()

# COMMAND ----------

df_remain.write.format("delta")\
            .mode("overwrite")\
            .option("path", error_table_path)\
            .saveAsTable(error_table_name)

# COMMAND ----------

df = spark.sql(f"select * from {error_table_name}")
display(df)
