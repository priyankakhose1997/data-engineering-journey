# Databricks notebook source
df = (spark.read                            # spak is session object, read is read object df reader
            .format("csv")                  #config and load the file into a dataframe
            .option("header", "true")       
            .option("inferSchema","true")
            .load("dbfs:/FileStore/accountdatadump.csv"))
display(df)

# COMMAND ----------

fire_df= (spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema","true")
                .load("dbfs:/FileStore/Fire_Department_30_day_calls_20250719.csv"))
#display(fire_df)

# COMMAND ----------

fire_df.createOrReplaceTempView('temp_view_name')
#select * from global_temp.temp_view_name

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists dev.databricks_demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_view_name
