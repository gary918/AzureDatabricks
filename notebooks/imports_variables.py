# Databricks notebook source
from pathlib import Path
import pandas as pd
import io
import tempfile
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

master_source_path ="/FileStore/error_handling/fruits.csv"
master_table_path = "/__data_storage/error_handling/fruits"
master_table_name = "default.fruits"
master_temp_view = "fruits_view"
master_sql = "select ID as id, shuiguo as fruit, yanse as color, jiage as price from fruits_view"

# COMMAND ----------

event_source_path = "/FileStore/error_handling/mock_events"
event_table_path = "/__data_storage__/error_handling/fruit_events"
event_table_ckpt_path = "/__data_storage__/error_handling/ckpt/fruit_events"
event_table_name = "default.fruit_events"
event_temp_view = "fruit_events_view"
event_sql = ""

# COMMAND ----------

bad_records_path = "/__data_storage__/error_handling/"
error_table_path = "/__data_storage__/error_handling/error_table"
error_table_ckpt_path = "/__data_storage__/error_handling/ckpt/error_table"
error_table_name = "default.error_table"
