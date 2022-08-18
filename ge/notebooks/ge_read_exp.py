# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Install Great Expectations

# COMMAND ----------

!pip install great_expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Sample Data

# COMMAND ----------

df = spark.sql(f"select * from default.fruit_events")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Form Dataset

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset
gdf = SparkDFDataset(df)
type(gdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate by the Existing Expectation

# COMMAND ----------

import great_expectations as ge

try:
    assert gdf.expect_column_values_to_be_between("price", 1, 5).success, "FAILED: Something wrong"
except AssertionError as e:
    print(e) 
#print(gdf.expect_column_values_to_be_between("price", 1, 10)["success"])
#print(gdf.expect_column_values_to_be_between("weight", 70, 500, mostly=0.8)["success"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read the Expectation Suite from a JSON File

# COMMAND ----------

validation_results = gdf.validate(expectation_suite="/dbfs/FileStore/expectations/my_exp.json")
#assert validation_results.success,"FAILD!!!"
print(validation_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save the Expectation Suite into a JSON File

# COMMAND ----------

gdf.save_expectation_suite("/dbfs/FileStore/expectations/my_expectations.json")

# COMMAND ----------

import json
  
# Opening JSON file
f = open("/dbfs/FileStore/expectations/my_expectations.json")
  
# returns JSON object as 
# a dictionary
data = json.load(f)
print(data)

# COMMAND ----------


