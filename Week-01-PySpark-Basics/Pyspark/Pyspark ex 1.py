# Databricks notebook source
df = (spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema","true")
            .load("dbfs:/FileStore/accountdatadump.csv"))
display(df)
