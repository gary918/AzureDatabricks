# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.fruits;
# MAGIC drop table if exists default.fruit_events;

# COMMAND ----------

dbutils.fs.rm(master_table_path,True)

# COMMAND ----------

dbutils.fs.rm("/error_handling",True)
