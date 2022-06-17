# Databricks notebook source
# MAGIC %run ./imports_variables

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(master_source_path)
display(df)

# COMMAND ----------

temp_view = "fruits_view"
sql = "select ID as id, shuiguo as fruit, yanse as color, jiage as price from fruits_view"
df.createOrReplaceTempView(master_temp_view)
df = spark.sql(sql)
display(df)


# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path",master_table_path)\
    .saveAsTable(master_table_name)

# COMMAND ----------

df = spark.sql(f"select * from {master_table_name}")
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC insert into default.fruits values("17","Pear","White",6.8)
