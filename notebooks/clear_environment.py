# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.fruits;
# MAGIC drop table if exists default.fruit_events;
# MAGIC drop table if exists default.error_table;

# COMMAND ----------

dbutils.fs.rm("/__data_storage__/error_handling",True)
