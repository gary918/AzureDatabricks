# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

# Load the master data from the CSV file
df = spark.read.format("csv").option("header", True).load(master_source_path)
display(df)

# COMMAND ----------

# Transform the master data
df.createOrReplaceTempView(master_data_tv)
df = spark.sql(master_transform_sql)
display(df)


# COMMAND ----------

# Save the transformed master data into the delta table
df.write.format("delta")\
    .mode("append")\
    .option("path",master_table_path)\
    .saveAsTable(master_table_name)

# COMMAND ----------

df = spark.sql(f"select * from {master_table_name}")
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Update the master data by using SQL
# MAGIC insert into default.fruits values("17","Watermelon","White",6.8)
