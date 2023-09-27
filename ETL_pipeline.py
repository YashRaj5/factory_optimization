# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

# DBTITLE 1,Ingest Raw Telemetry from PPlants
from pyspark.sql.functions import *
import dlt
 
@dlt.create_table(name="OEE_bronze", 
                  comment = "Raw telemetry from factory floor", 
                  spark_conf={"pipelines.trigger.interval" : "1 seconds", "spark.databricks.delta.schema.autoMerge.enabled":"false"})
def get_raw_telemetry():
  df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.maxFilesPerTrigger", 16)
      .load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/incoming_sensors"))
  return df.withColumn("body", col("body").cast('string'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

#get the schema we want to extract from a json example
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
example_body = """{
  "applicationId":"3e9449fe-3df7-4d06-9375-5ee9eeb0891c",
  "deviceId":"Everett-BoltMachine-2",
  "messageProperties":{"iothub-connection-device-id":"Everett-BoltMachine-2","iothub-creation-time-utc":"2022-05-03T17:05:26-05:00","iothub-interface-id":""},
  "telemetry": {"batchNumber":23,"cpuLoad":3.03,"defectivePartsMade":2,"machineHealth":"Healthy","memoryFree":211895179,"memoryUsed":56540277,
   "messageTimestamp":"2022-05-03T22:05:26.402569Z","oilLevel":97.50000000000014,"plantName":"Everett","productionLine":"ProductionLine 2",
   "shiftNumber":3,"systemDiskFreePercent":75,"systemDiskUsedPercent":25,"temperature":89.5,"totalPartsMade":99}}"""
  
EEO_schema = spark.read.json(sc.parallelize([example_body])).schema

# COMMAND ----------

silver_rules = {"positive_defective_parts":"defectivePartsMade >= 0",
                "positive_parts_made":"totalPartsMade >= 0",
                "positive_oil_level":"oilLevel >= 0",
                "expected_shifts": "shiftNumber >= 1 and shiftNumber <= 3"}
 
@dlt.create_table(comment = "Process telemetry from factory floor into Tables for Analytics",
                  table_properties={"pipelines.autoOptimize.managed": "true"})
@dlt.expect_all_or_drop(silver_rules)
def OEE_silver():
  return (dlt.read_stream('OEE_bronze')
             .withColumn("jsonData", from_json(col("body"), EEO_schema)) 
             .select("jsonData.applicationId", "jsonData.deviceId", "jsonData.messageProperties.*", "jsonData.telemetry.*") 
             .withColumn("messageTimestamp", to_timestamp(col("messageTimestamp"))))

# COMMAND ----------

# DBTITLE 1,Ingest Employee count data
@dlt.create_table(comment="count of workforce for a given shift read from delta")
def workforce_silver():
  return spark.read.format("delta").load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/workforce")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

# DBTITLE 1,Define our data quality expectation as SQL functions
gold_rules = {"warn_defective_parts":"defectivePartsMade < 35", 
              "warn_low_temperature":"min_temperature > 70", 
              "warn_low_oilLevel":"min_oilLevel > 60", 
              "warn_decrease_Quality": "Quality > 99", 
              "warn_decrease_OEE": "OEE > 99.3"}

# COMMAND ----------

import pyspark.sql.functions as F
 
@dlt.create_table(name="OEE_gold",
                 comment="Aggregated equipment data from shop floor with additional KPI metrics like OEE",
                 spark_conf={"pipelines.trigger.interval" : "1 minute"},
                 table_properties={"pipelines.autoOptimize.managed": "true"})
@dlt.expect_all(gold_rules)
def create_agg_kpi_metrics():
  bus_agg = (dlt.read_stream("OEE_silver")
          .groupby(F.window(F.col("messageTimestamp"), "5 minutes"), "plantName", "productionLine", "shiftNumber")
          .agg(F.mean("systemDiskFreePercent").alias("avg_systemDiskFreePercent"),
               F.mean("oilLevel").alias("avg_oilLevel"), F.min("oilLevel").alias("min_oilLevel"),F.max("oilLevel").alias("max_oilLevel"),
               F.min("temperature").alias("min_temperature"),F.max("temperature").alias("max_temperature"),
               F.mean("temperature").alias("avg_temperature"),F.sum("totalPartsMade").alias("totalPartsMade"),
               F.sum("defectivePartsMade").alias("defectivePartsMade"),
               F.sum(F.when(F.col("machineHealth") == "Healthy", 1).otherwise(0)).alias("healthy_count"), 
               F.sum(F.when(F.col("machineHealth") == "Error", 1).otherwise(0)).alias("error_count"),
               F.sum(F.when(F.col("machineHealth") == "Warning", 1).otherwise(0)).alias("warning_count"), F.count("*").alias("total_count")
          )
          .withColumn("Availability", (F.col("healthy_count")-F.col("error_count"))*100/F.col("total_count"))
          .withColumn("Quality", (F.col("totalPartsMade")-F.col("defectivePartsMade"))*100/F.col("totalPartsMade"))
          .withColumn("Performance", (F.col("healthy_count")*100/(F.col("healthy_count")+F.col("error_count")+F.col("warning_count"))))
          .withColumn("OEE", (F.col("Availability")+ F.col("Quality")+F.col("Performance"))/3))
  return bus_agg.join(dlt.read("workforce_silver"), ["shiftNumber"], "inner")
