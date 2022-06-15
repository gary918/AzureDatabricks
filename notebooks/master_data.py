# Databricks notebook source
from pathlib import Path
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import io
import tempfile
import pyspark.sql.functions as F

# COMMAND ----------

data_source_path ="/FileStore/fruits.csv"

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(data_source_path)
display(df)

# COMMAND ----------

temp_view = "fruits_view"
sql = "select ID as id, shuiguo as fruit, yanse as color, jiage as price from fruits_view"
df.createOrReplaceTempView(temp_view)
df = spark.sql(sql)
display(df)


# COMMAND ----------

table_path = "/__data_storage__/fruits"
table_name = "default.fruits"
df.write.format("delta").mode("append").option("path",table_path).saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.fruits
