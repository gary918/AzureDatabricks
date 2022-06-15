# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.fruits;

# COMMAND ----------

dbutils.fs.rm(table_path,True)
