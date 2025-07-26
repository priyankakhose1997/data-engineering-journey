-- Databricks notebook source
-- MAGIC %python
-- MAGIC df = (spark.read                            # spak is session object, read is read object df reader
-- MAGIC             .format("csv")                  #config and load the file into a dataframe
-- MAGIC             .option("header", "true")       
-- MAGIC             .option("inferSchema","true")
-- MAGIC             .load("dbfs:/FileStore/accountdatadump.csv"))
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df= (spark.read
-- MAGIC                 .format("csv")
-- MAGIC                 .option("header", "true")
-- MAGIC                 .option("inferSchema","true")
-- MAGIC                 .load("dbfs:/FileStore/Fire_Department_30_day_calls_20250719.csv"))
-- MAGIC #display(fire_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df.createOrReplaceTempView('temp_view_name')
-- MAGIC #select * from global_temp.temp_view_name

-- COMMAND ----------

create table if not exists dev.databricks_demo.fire_service_calls
(
`Station ID` integer,
`Call Type` string,
`Total Emergency Calls` integer,
`Total Non-emergency Calls` integer,
`Emergency Response` string,
`Non-emergency Response` string,
`Data As Of` date,
`Data Loaded At` string
) using delta

-- COMMAND ----------

select * from dev.databricks_demo.fire_service_calls
