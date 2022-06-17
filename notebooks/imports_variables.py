# Databricks notebook source
from pathlib import Path
import pandas as pd
import io
import tempfile
from delta.tables import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

master_source_path ="/FileStore/error_handling/fruits.csv"
master_table_path = "/__data_storage__/error_handling/fruits"
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
valid_event_data_tv = "valid_fruit_events_view"
invalid_event_data_tv = "invalid_fruit_events_view"
event_valid_sql = f'select * from {event_temp_view} e where e.id in (select m.id from {master_table_name} as m)'
event_invalid_sql = f'select * from {event_temp_view} e where e.id not in (select m.id from {master_table_name} as m)'

# COMMAND ----------

def validate_records():
    valid_records = spark.sql(event_valid_sql)
    invalid_records = spark.sql(event_invalid_sql)
    return valid_records, invalid_records
