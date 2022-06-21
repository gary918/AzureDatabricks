# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

df = spark.sql(f"select * from {master_table_name}")
display(df)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/FileStore/error_handling/mock_events/file_0.csv

# COMMAND ----------

df = spark.sql(f"select * from {event_table_name}")
display(df)

# COMMAND ----------

df = spark.sql(f"select * from {error_table_name}")
display(df)

# COMMAND ----------


