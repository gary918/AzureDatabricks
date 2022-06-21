# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

dbutils.widgets.text('count', '')
count = int(dbutils.widgets.get('count'))

# COMMAND ----------

import csv
import uuid
from random import *
import time
from pathlib import Path
from datetime import datetime

# count = 0
Path(event_source_path).mkdir(parents=True, exist_ok=True)

row_list = [ ["id", "amount", "ts"],
               [randint(1,7), randint(1,20), datetime.now()],
               [randint(8,20), randint(1,20), datetime.now()],
               [randint(1,7), randint(1,20), datetime.now()],
               [randint(1,7), randint(1,20), 999],
               [randint(1,7), randint(1,20)]
             ]
file_location = f'{event_source_path}/file_{count}.csv'

with open(file_location, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(row_list)
    file.close()

count += 1
dbutils.fs.mv(f'file:{file_location}', f'dbfs:{file_location}')
time.sleep(5)
print(f'New CSV file created at dbfs:{file_location}. Contents:')

with open(f'/dbfs{file_location}', 'r') as file:
    reader = csv.reader(file, delimiter=' ')
    for row in reader:
      print(', '.join(row))
    file.close()
