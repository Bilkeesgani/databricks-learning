# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark Basics Practice

# COMMAND ----------

# Sample DataFrame banao
data = [
    (1, "Ali", 25, "Karachi"),
    (2, "Sara", 30, "Lahore"),
    (3, "Ahmed", 28, "Islamabad")
]

df = spark.createDataFrame(data, ["id", "name", "age", "city"])
df.show()



# COMMAND ----------

# Filter operation
df_filtered = df.filter(df.age > 26)
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Tasks
# MAGIC 1. Count total rows
# MAGIC 2. Find average age
# MAGIC 3. Group by city
