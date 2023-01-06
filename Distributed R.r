# Databricks notebook source
# MAGIC %md
# MAGIC ###Apache Spark
# MAGIC Apache Spark is an open source analytics engine used for big data workloads. It is a sophisticated distributed computation framework for executing code in parallel across many different machines. It can handle both batches as well as real-time analytics and data processing workloads.
# MAGIC 
# MAGIC ![Spark Cluster Mode](files/srijit/demo/images/Spark_Architecture.png)
# MAGIC 
# MAGIC 
# MAGIC In a Spark dataframe, the data is partitioned into smaller subsets and distributed across worker nodes. The processing then happens on this distributed data parallely. Finally the results can be collected to the driver node
# MAGIC 
# MAGIC ![Spark Dataframe](files/srijit/demo/images/Spark_Parallelism.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Runtime with Apache Spark
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/11/Runtime-OG.png" width="600"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Runtime improves the user experience of developing in Apache Spark by offering better performance, reliability, and security.  This is achieved by a series of optimizations and service features, such as: <br><br>
# MAGIC 
# MAGIC * **Databricks I/O** - Higher S3/Blob storage throughput, data skipping, transparent caching
# MAGIC * **Serverless Infrastructure** - Shared pools, auto-configuration, auto-scaling, reliable fine-grained sharing
# MAGIC 
# MAGIC Practically speaking, what this means is that you can port your existing Spark code to Databricks Runtime and [expect it to perform better](https://databricks.com/blog/2017/10/06/accelerating-r-workflows-on-databricks.html).  Now, if you are an R developer writing Spark jobs, chances are you are going to be writing that code in one of two APIs for Spark.  Let's cover those now.

# COMMAND ----------

# MAGIC %md
# MAGIC ####SparkR
# MAGIC 
# MAGIC [SparkR](https://spark.apache.org/docs/latest/sparkr.html) is an R package that provides a light-weight frontend to use Apache Spark from R. In Spark 3.3.1, SparkR provides a distributed data frame implementation that supports operations like selection, filtering, aggregation etc. (similar to R data frames, dplyr) but on large datasets. SparkR also supports distributed machine learning using MLlib.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sparklyr
# MAGIC [SparklyR](https://spark.rstudio.com/) is an R interface to Spark, developed by RStudio. It lets you run distributed code in Spark and give access to Spark’s distributed Machine Learning libraries, Structure Streaming,and ML Pipelines from R.

# COMMAND ----------

# MAGIC %md
# MAGIC ####SparkR vs SparklyR
# MAGIC 
# MAGIC So which one should you choose? 
# MAGIC 
# MAGIC It really is mostly down to personal preference. If you are already used to working with tidyverse, then it's probably easiest to just stick with Sparklyr, as the syntax is very similar. Sparklyr also allows for chaining operations more easily, which is not straightforward with SparkR. SparkR tends to however give slightly better performance.
# MAGIC 
# MAGIC Both libraries are highly capable of working with big data in R - as of Feb. '19 their feature sets are essentially at parity.  Perhaps the biggest difference between the two is `sparklyr` was designed using the `dplyr` approach to data analysis.  The consequences of this are that `sparklyr` functions return a _reference_ to a Spark DataFrame which can be used as a `dplyr` table ([source](https://spark.rstudio.com/dplyr/)).

# COMMAND ----------

# MAGIC %md
# MAGIC Calling `SparkR` functions on `sparklyr` objects and vice versa can lead to unexpected -- and undesirable -- behavior.  Why is this?
# MAGIC 
# MAGIC `dplyr` functions that work with Spark are translated into SparkSQL statements, and always return a SparkSQL table.  This is **not** the case with the `SparkR` API, which has functions for SparkSQL tables and Spark DataFrames.  As such, the interoperability between the two APIs is limited and it is generally not recommended to go back and forth between them in the same job. 

# COMMAND ----------


