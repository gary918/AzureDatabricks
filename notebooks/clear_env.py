# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.fruits;

# COMMAND ----------

dbutils.fs.rm("/__data_storage__/error_handling",True)

# COMMAND ----------

dbutils.fs.rm(master_table_path,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.fruit_events;
# MAGIC drop table if exists default.error_table;

# COMMAND ----------

dbutils.fs.rm(event_source_path,True)
dbutils.fs.rm(event_table_path,True)
dbutils.fs.rm(event_table_ckpt_path,True)
dbutils.fs.rm(error_table_path,True)
dbutils.fs.rm(error_table_ckpt_path,True)

# COMMAND ----------

dbutils.fs.rm(master_table_path,True)

# COMMAND ----------


