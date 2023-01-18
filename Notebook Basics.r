# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Notebook Basics

# COMMAND ----------

# MAGIC %md
# MAGIC ###Development Using Notebooks
# MAGIC 
# MAGIC * Databricks File System -- DBFS
# MAGIC * Magic Commands -- %python, %sql, and more
# MAGIC * `dbutils` -- Handy functions to make you more productive
# MAGIC * Widgets -- Paramaterize your notebook
# MAGIC * Installing Packages
# MAGIC * Exploratory Data Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ####DBFS
# MAGIC 
# MAGIC If you are wondering whether you have a file system to work with, the answer is yes!
# MAGIC 
# MAGIC Databricks File System (DBFS) is a distributed file system installed on Databricks clusters. Files in DBFS persist to object storage, so you won’t lose data even after you terminate a cluster.  Even better, you get the benefits of flexible cloud storage and the organizing structure of a file system.
# MAGIC 
# MAGIC Let's take a look at what we find in the `/rworkshop/files` directory on DBFS.

# COMMAND ----------

# DBFS is mounted to the local file system on the cluster
system("ls /dbfs/rworkshop/files", intern = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can read more about DBFS in the [official Databricks docs](https://docs.databricks.com/data/databricks-file-system.html#databricks-file-system-dbfs).

# COMMAND ----------

# MAGIC %md
# MAGIC ####Magic Commands
# MAGIC 
# MAGIC You can override the primary language of a notebook by specifying the language magic command %<language> at the beginning of a cell. The supported magic commands are: `%python`,`%r`, `%scala`, and `%sql`. In addition, you can run shell commands using `%sh`,  document your work with Markdown syntax by using `%md`, and execute other notebooks using `%run`.  
# MAGIC   
# MAGIC These magic commands are a powerful way to open up your notebook to specialized libraries in other languages as well as assets you may have created in other notebooks.  For example, save some `ggplot2` or `plotly` output to DBFS and render them in a new notebook using Markdown.  To dive deeper please see the [official documentation for Databricks Notebooks](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html).
# MAGIC   
# MAGIC Let's try some of them

# COMMAND ----------

# MAGIC %python
# MAGIC def printHello(name):
# MAGIC   print(f"Hello {name}")
# MAGIC   
# MAGIC   
# MAGIC current_user = username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# MAGIC printHello(current_user)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This cell was created using an `%md` magic command

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def printHello(name:String){
# MAGIC   println("Hello "+name)
# MAGIC }
# MAGIC 
# MAGIC val userName = dbutils.notebook.getContext().userName.getOrElse("")
# MAGIC 
# MAGIC printHello(userName)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/dbfs/rworkshop/files

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC %run
# MAGIC * You can run a notebook from another notebook by using the magic command **%run**
# MAGIC * Notebooks to be run are specified with relative paths
# MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

# COMMAND ----------

# MAGIC %run ./run_test

# COMMAND ----------

print(run_test_var)

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ####Databricks Utilities
# MAGIC 
# MAGIC Databricks Utilities (`dbutils`) are a set of functions that designed to empower you and your productivity inside of Databricks.
# MAGIC 
# MAGIC With the exception of [file system utilities](https://docs.databricks.com/dev-tools/databricks-utils.html#dbutils-fs), all `dbutils` functions are available for R notebooks. For example, [secrets utilities](https://docs.azuredatabricks.net/user-guide/dev-tools/dbutils.html#secrets-utilities) let you work with authentication variables (like JDBC credentials) without having to display sensitive information in your notebook:

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Widgets
# MAGIC 
# MAGIC One particularly useful set of `dbutils` functions are [Widgets](https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#widgets).  Widgets add a layer of parameterization to your notebooks that can be used when building dashboards and visualizations, training models, or querying data sources.  Think of widgets as turning your notebook into a function.
# MAGIC 
# MAGIC In the following example we'll create a widget to dynamically render the output of a plot.

# COMMAND ----------

airline_data <- read.csv('/dbfs/dbfs/rworkshop/files/2008.csv')

# COMMAND ----------

display(airline_data)

# COMMAND ----------

library(ggplot2)
library(dplyr)

# COMMAND ----------

## Define the choices for our dropdown widget
orig_list <- as.list(unique(airline_data$Origin))
dest_list <- as.list(unique(airline_data$Dest))

## Instatiate the widgets
dbutils.widgets.dropdown(name = "Origin", defaultValue = "ABE", choices = orig_list)
dbutils.widgets.dropdown(name = "Destination", defaultValue = "ATL", choices = dest_list)

## Store our widget value
origin_value <- dbutils.widgets.get("Origin")
destination_value <- dbutils.widgets.get("Destination")

result_data <- airline_data[airline_data$Origin == origin_value & airline_data$Dest == destination_value, ]

## Quick plot of mtcars using dynamic widget values 
#plot(x = result_data[, 'date'], xlab = "Date",
#     y = result_data[, 'delay'], ylab = "Delay")

ggplot2::ggplot(result_data, ggplot2::aes( x= DepDelay) ) +
  geom_histogram()

# COMMAND ----------

# MAGIC %md
# MAGIC Changing the values in each widget will automatically rerun the cell and render fresh output.  Give it a shot!

# COMMAND ----------

# MAGIC %md
# MAGIC ####Visualization
# MAGIC 
# MAGIC We can also perform data aggregations and visualization using ggplot

# COMMAND ----------

## Group the data by carrier and day of week, get the sum of Distance for each group
dist_agg_df <- summarize(group_by(airline_data, UniqueCarrier, DayOfWeek), DistanceTotal = sum(Distance))

## Plot
options(repr.plot.width=3600, repr.plot.height=1200)

p <- ggplot2::ggplot(dist_agg_df, ggplot2::aes(x = UniqueCarrier, y = DistanceTotal/1000000, fill = as.factor(DayOfWeek))) + 
        ggplot2::geom_bar(stat = "identity", position = "dodge") +
        ggplot2::geom_bar(stat = "identity", position = 'dodge', color = 'white') +
        ggplot2::scale_y_continuous(labels = scales::unit_format(unit = "M")) + 
        ggplot2::scale_fill_discrete(name = "Day of Week") +
        ggplot2::labs(y = 'Distance') +
        ggplot2::theme_minimal() 

p

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Lets remove all the widgets

# COMMAND ----------

## Remove all widgets from this notebook
dbutils.widgets.removeAll()

# COMMAND ----------


