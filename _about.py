# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Scenario - Track KPIs to increase plant capacity 
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_production_factories.png" width="600px" />
# MAGIC
# MAGIC An Aerospace manufacturing company has launched a Factory of the Future manufacturing initiative to streamline operations and increase production capacity at its plants and production lines. 
# MAGIC
# MAGIC Leveraging a __Lakehouse Architecture__ *Plant Managers* can assess __KPIs__ to  calculate shift effectiveness and communicate with equipment operators and then adjust the factory equipment accordingly.
# MAGIC
# MAGIC This requires real-time KPIs tracking based on multiple datasources, including our factories sensor and external sources, to answer questions such as:
# MAGIC
# MAGIC * What's my current availability
# MAGIC * What was my past Performance
# MAGIC * Do I have a factory under-performing and perform root cause analysis
# MAGIC * ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why OEE?
# MAGIC Overall Equipment Effectiveness (OEE) is a measure of how well a manufacturing operation is utilized (facilities, time and material) compared to its full potential, during the periods when it is scheduled to run. [References](https://en.wikipedia.org/wiki/Overall_equipment_effectiveness). 
# MAGIC
# MAGIC OEE is the industry standard for measuring manufacturing productivity. OEE is calculated using 3 attributes
# MAGIC
# MAGIC 1. **Availability:** accounts for planned and unplanned stoppages, percentage of scheduled time that the operation is/was available to operate. *i.e.* __(Healthy_time - Error_time)/(Total_time)__
# MAGIC 2. **Performance:** measure of speed at which the work happens, percentage of its designed speed. *i.e.* __Healthy_time/ Total_time__
# MAGIC 3. **Quality:** percentage of good units produced compared to the total units planned/produced. *i.e.* __(Total Parts Made -  Defective Parts Made)/Total Parts Made__
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Medallion Architecture for IOT data
# MAGIC
# MAGIC Fundamental to the lakehouse view of ETL/ELT is the usage of a multi-hop data architecture known as the medallion architecture.
# MAGIC
# MAGIC This is the flow we'll be implementing.
# MAGIC
# MAGIC - Incremental ingestion of data from the sensor / IOT devices
# MAGIC - Cleanup the data and extract required information
# MAGIC - Consume our workforce dataset coming from our SalesForce integration
# MAGIC - Merge both dataset and compute real-time aggregation based on a temporal window.
# MAGIC
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_0.png" width="1000px"/>
# MAGIC
# MAGIC
# MAGIC Databricks Lakehouse let you do all in one open place, without the need to move the data into a proprietary data warehouse - thus maintaining coherency and a single source of truth for our data.
# MAGIC
# MAGIC In addition to this pipeline, predictive analysis / forecast can easily be added in this pipeline to add more values, such as:
# MAGIC
# MAGIC * Predictive Maintenance
# MAGIC * Anomaly detection
# MAGIC * ...

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ingesting Sensor JSON payloads and saving them as our first table
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_1.png" width="600px"/>
# MAGIC
# MAGIC Our raw data is being streamed from the device and sent to a blob storage. 
# MAGIC
# MAGIC Autoloader simplify this ingestion by allowing incremental processing, including schema inference, schema evolution while being able to scale to millions of incoming files. 
# MAGIC
# MAGIC Autoloader is available in __SQL & Python__ using the `cloud_files` function and can be used with a variety of format (json, csv, binary, avro...). It provides
# MAGIC
# MAGIC - Schema inference and evolution
# MAGIC - Scalability handling million of files
# MAGIC - Simplicity: just define your ingestion folder, Databricks take care of the rest!
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Silver layer: transform JSON data into tabular table 
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_2.png" width="600px"/>
# MAGIC
# MAGIC The next step is to cleanup the data we received and extract the important field from the JSON data.
# MAGIC
# MAGIC This will increase performance and make the table schema easier to discover for external users, allowing to cleanup any missing data in the process.
# MAGIC
# MAGIC We'll be using Delta Live Table Expectations to enforce the quality in this table.
# MAGIC
# MAGIC In addition, let's enabled the managed autoOptimized property in DLT. The engine will take care of compaction out of the box.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ingesting the Workforce dataset
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_3.png" width="600px"/>
# MAGIC
# MAGIC We'll then ingest the workforce data. Again, we'll be using the autoloader.
# MAGIC
# MAGIC *Note that we could have used any other input source (kafka etc.)*
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Gold layer: Calculate KPI metrics such as OEE, Availability, Quality and Performance
# MAGIC
# MAGIC We'll compute these metrics for all plants under in our command center leveraging structured streamings *Stateful Aggregation*. 
# MAGIC
# MAGIC We'll do a sliding window aggregation based on the time: we'll compute average / min / max stats for every 5min in a sliding window and output the results to our final table.
# MAGIC
# MAGIC Such aggregation can be really tricky to write. You have to handle late message, stop/start etc.
# MAGIC
# MAGIC But Databricks offer a full abstraction on top of that. Just write your query and the engine will take care of the rest.
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_4.png" width="600px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## KPI visualization with Databricks SQL
# MAGIC
# MAGIC That's it! Our KPIs will be refreshed in real-time and ready to be analyzed.
# MAGIC
# MAGIC We can now start using Databricks SQL Dashboard or any of your usual BI tool (PowerBI, Tableau...) to build our Manufacturing Tower Control
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_dashboard.png" width="600px"/>
# MAGIC
# MAGIC ## Next step
# MAGIC
# MAGIC Dashboard Analysis is just the first step of our Data Journey. Databricks Lakehouse let you build more advanced data use-cases such as Predictive models and setup predictive analysis pipeline.
# MAGIC
