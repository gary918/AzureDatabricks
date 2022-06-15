# Databricks notebook source
from pathlib import Path
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import io
import tempfile
import pyspark.sql.functions as F

# COMMAND ----------

master_source_path ="/FileStore/fruits.csv"
master_table_path = "/__data_storage__/fruits"
master_table_name = "default.fruits"
master_temp_view = "fruits_view"
master_sql = "select ID as id, shuiguo as fruit, yanse as color, jiage as price from fruits_view"

# COMMAND ----------

event_source_path = "/FileStore/events.csv"
event_table_path = "/__data_storage__/fruit_events"
event_table_name = "default.fruit"
event_temp_view = "fruit_events_view"
event_sql = ""
