# Databricks notebook source
# MAGIC %md
# MAGIC # How to Show Data in Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Directly

# COMMAND ----------

import time
res = {'1': [8601, 10, 385.58, 11, 'MSFT'],
        '2': [7007, 1, 210.23, 6, 'APPL'],
        '3': [1209, 1, 336.18, 7, 'AWS'],
        '4': [9, 9, 148.98, 11, 'GOOGL'],
        '5': [3141, 8, 208.93, 17, 'META']}
i = 0
while i<10:
    res['3']=[i,i,i,'AWS']
    print(res, end='\r')
    time.sleep(1)
    i+=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print the Pandas Dataframe

# COMMAND ----------

import pandas as pd
from IPython.display import display, clear_output
import time
res = {'1': [8601, 10, 385.58, 11, 'MSFT'],
        '2': [7007, 1, 210.23, 6, 'APPL'],
        '3': [1209, 1, 336.18, 7, 'AWS'],
        '4': [9, 9, 148.98, 11, 'GOOGL'],
        '5': [3141, 8, 208.93, 17, 'META']}

i = 0
while i<10:
    # Convert the dictionary into a DataFrame
    res['3'] = [i,i,i,i,'AWS']
    df = pd.DataFrame.from_dict(res, orient='index', columns=['PRICE', 'SHARE', 'FORECAST', 'SIZE', 'CODE'])

    # Display the DataFrame in the notebook
    clear_output(wait=True)
    print(df)
    time.sleep(1)
    i +=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use PrettyTable

# COMMAND ----------

!pip install prettytable

# COMMAND ----------

# On Azure Databricks, the solution can refresh the table content
from prettytable import PrettyTable
import time
import os
from IPython.display import display,clear_output

my_dict = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

# Create a table with two columns
table = PrettyTable(['Key', 'Value'])

# Print the table header
print(table)

i = 0
while i<10:
    os.system('cls' if os.name == 'nt' else 'clear')
    # Clear the existing rows from the table
    table.clear_rows()

    my_dict['key1']=str(i)

    # Iterate over the key-value pairs in the dictionary
    for key, value in my_dict.items():
        # Add the key-value pair to the table
        table.add_row([key, value])

    # Print the updated table
    clear_output(wait=True)
    print(table)

    # Wait for some time before refreshing the table
    time.sleep(1)
    i = i+1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use display

# COMMAND ----------

# On Azure Databricks, the output can be refreshed
# Stacks lines in the output on Azure Synapse
from random import uniform
import time
from IPython.display import display, clear_output

i = 1
while i<10:
    clear_output(wait=True)
    display('Iteration '+str(i)+' Score: '+str(uniform(0, 1)))
    time.sleep(1)
    i += 1

# COMMAND ----------

# Only displays the table at the end on Azure Databricks
# Stacks the tables in the output on Azure Synapse
import pandas as pd
from IPython.display import display, clear_output
res = {'1': [8601, 10, 385.58, 11, 'MSFT'],
        '2': [7007, 1, 210.23, 6, 'APPL'],
        '3': [1209, 1, 336.18, 7, 'AWS'],
        '4': [9, 9, 148.98, 11, 'GOOGL'],
        '5': [3141, 8, 208.93, 17, 'META']}

i = 0
while i<10:
    # Convert the dictionary into a DataFrame
    res['3'] = [i,i,i,i,'AWS']
    df = pd.DataFrame.from_dict(res, orient='index', columns=['PRICE', 'SHARE', 'FORECAST', 'SIZE', 'CODE'])

    # Display the DataFrame in the notebook
    clear_output(wait=True)
    display(df)
    time.sleep(1)
    i +=1
