# Databricks notebook source
# MAGIC %md
# MAGIC ####SparkR
# MAGIC 
# MAGIC In this exercise we will
# MAGIC - load the public census data provided by American Community Survey as [Summary Files](https://www.census.gov/programs-surveys/acs/data/summary-file.2019.html#list-tab-1622397667)
# MAGIC - Perform some data wrangling
# MAGIC - Persist the data to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Data
# MAGIC We will load the public census data provided by American Community Survey as [Summary Files](https://www.census.gov/programs-surveys/acs/data/summary-file.2019.html#list-tab-1622397667) and perform some data wrangling on the data

# COMMAND ----------

#TODO Replace EEE with your email
root_dbfs <- "/dbfs/rworkshop"
user_email <- EEE

root_user_dbfs <- paste0(root_dbfs,"/",user_email)
database_directory <- paste0(root_user_dbfs,"/data")
mlflow_directory <- paste0(root_user_dbfs,"/mlflow")
files_directory <-  paste0(root_dbfs,"/files")

print(paste0(" Database directory: ", database_directory))
print(paste0(" MLFlow directory: ", mlflow_directory))
print(paste0(" Files directory: ", files_directory))

dbutils.fs.mkdirs(database_directory)
dbutils.fs.mkdirs(mlflow_directory)

# COMMAND ----------

data_2018 <- paste0("dbfs:/",files_directory,"/Geos20181YR.csv")
data_2019 <- paste0("dbfs:/",files_directory,"/Geos20191YR.csv")

# COMMAND ----------

library(magrittr)
library(SparkR)
sparkR.session()

# COMMAND ----------

#the repartition statement is optional.. since data size is small, it might keep in everything in driver
df_2018 <- read.df(data_2018, source="csv",header="true", repartition=40)
df_2019 <- read.df(data_2019, source="csv",header="true", repartition=40)

# COMMAND ----------

colnames(df_2018)

# COMMAND ----------

nrow(df_2018)

# COMMAND ----------

nrow(df_2019)

# COMMAND ----------

display(head(df_2018, 1000))

# COMMAND ----------

# examine the structure of the dataframe
str(df_2018)

# COMMAND ----------

#print the schema of the dataframe
printSchema(df_2018)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Process Data
# MAGIC We can now start working on the data using SparkR dataframe apis. A comprehensive documentation of all methods are available [here](https://spark.apache.org/docs/3.2.0/api/R/index.html).

# COMMAND ----------

#TODO We need to rename the default aggregate column count to NUMURB_2018
urb_count_2018 <- df_2018 %>% 
  select(c("STUSAB","UA") ) %>% 
  groupBy("STUSAB") %>% 
  count() %>% 
  #withColumnRenamed(existingCol="count", newCol="NUMURB_2018")

# COMMAND ----------

display(head(urb_count_2018,100))

# COMMAND ----------

#TODO We need to rename the default column count to NUMURB_2019

urb_count_2019 <- df_2019 %>% 
  select(c("STUSAB","UA") ) %>% 
  groupBy("STUSAB") %>% 
  count() %>% 
  #withColumnRenamed(existingCol="count", newCol="NUMURB_2019")  

# COMMAND ----------

display(head(urb_count_2019,100))

# COMMAND ----------

#TODO We need to add a condition to determine the urban area that had some growth
#TODO also sort the result with highest growing urban area on top
result <- urb_count_2018 %>%
  merge(urb_count_2019, by="STUSAB" ) %>%
  select(c("STUSAB_x","NUMURB_2018","NUMURB_2019")) %>%
  withColumn("URBAN_GROWTH", (.$NUMURB_2019 - .$NUMURB_2018)) %>%
  #filter(.$URBAN_GROWTH != 0) %>%
  #arrange("URBAN_GROWTH", decreasing=TRUE)


# COMMAND ----------

head(result,100)

# COMMAND ----------

class(result)

# COMMAND ----------

collect(summary(result))

# COMMAND ----------

collect(describe(result))

# COMMAND ----------

# MAGIC %md
# MAGIC **REMEMBER**: `collect()`will bring the results of the operation to the driver node.
# MAGIC Large datasets will exceed the memory of the driver node and produce an OOM error.

# COMMAND ----------

r_result <- collect(result)

# COMMAND ----------

class(r_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Save Results to Delta

# COMMAND ----------

#TODO Fill out XXX with table name Add your initials to avoid conflict eg: 'survey_result_YOUR INITIALS'
survey_delta_table_name <- XXX

survey_table_location <- paste0("dbfs:",database_directory, "/",survey_delta_table_name)

print(paste0("Location for table ",survey_delta_table_name," is: ",survey_table_location))

# COMMAND ----------

#if everything looks good, let us write the data
write.df(result, path=survey_table_location)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can create an external table in the metastore with this data, so that everyone can use it

# COMMAND ----------

# MAGIC %sql
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC USE DATABASE rworkshop;

# COMMAND ----------

sql(paste0("DROP TABLE IF EXISTS ",survey_delta_table_name))

# COMMAND ----------

sql(paste0("CREATE TABLE IF NOT EXISTS ",survey_delta_table_name," USING delta LOCATION '",survey_table_location,"'"))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see the table in data explorer
# MAGIC 
# MAGIC 
# MAGIC Now we can query the data from anywhere

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO Replace XXX with the table name
# MAGIC SELECT * FROM XXX

# COMMAND ----------

# MAGIC %md
# MAGIC ####ML with SparkR

# COMMAND ----------

# MAGIC %md
# MAGIC SparkR supports the following machine learning algorithms currently:
# MAGIC 
# MAGIC **Classification**
# MAGIC ```
# MAGIC spark.logit: Logistic Regression
# MAGIC spark.mlp: Multilayer Perceptron (MLP)
# MAGIC spark.naiveBayes: Naive Bayes
# MAGIC spark.svmLinear: Linear Support Vector Machine
# MAGIC spark.fmClassifier: Factorization Machines classifier
# MAGIC ```
# MAGIC **Regression**
# MAGIC ```
# MAGIC spark.survreg: Accelerated Failure Time (AFT) Survival Model
# MAGIC spark.glm or glm: Generalized Linear Model (GLM)
# MAGIC spark.isoreg: Isotonic Regression
# MAGIC spark.lm: Linear Regression
# MAGIC spark.fmRegressor: Factorization Machines regressor
# MAGIC ```
# MAGIC 
# MAGIC **Tree**
# MAGIC ```
# MAGIC spark.decisionTree: Decision Tree for Regression and Classification
# MAGIC spark.gbt: Gradient Boosted Trees for Regression and Classification
# MAGIC spark.randomForest: Random Forest for Regression and Classification
# MAGIC ```
# MAGIC 
# MAGIC **Clustering**
# MAGIC ```
# MAGIC spark.bisectingKmeans: Bisecting k-means
# MAGIC spark.gaussianMixture: Gaussian Mixture Model (GMM)
# MAGIC spark.kmeans: K-Means
# MAGIC spark.lda: Latent Dirichlet Allocation (LDA)
# MAGIC spark.powerIterationClustering (PIC): Power Iteration Clustering (PIC)
# MAGIC ```
# MAGIC 
# MAGIC **Collaborative Filtering**
# MAGIC ```
# MAGIC spark.als: Alternating Least Squares (ALS)
# MAGIC Frequent Pattern Mining
# MAGIC spark.fpGrowth : FP-growth
# MAGIC spark.prefixSpan : PrefixSpan
# MAGIC ```
# MAGIC 
# MAGIC **Statistics**
# MAGIC ```
# MAGIC spark.kstest: Kolmogorov-Smirnov Test
# MAGIC ```
# MAGIC 
# MAGIC Under the hood, SparkR uses MLlib to train the model. Please refer to the corresponding section of MLlib user guide for example code. Users can call summary to print a summary of the fitted model, predict to make predictions on new data, and write.ml/read.ml to save/load fitted models. SparkR supports a subset of the available R formula operators for model fitting, including ‘~’, ‘.’, ‘:’, ‘+’, and ‘-‘.

# COMMAND ----------

# MAGIC %md
# MAGIC Let us try to run a classification model on iris dataset

# COMMAND ----------

# let's read the dataset from the Databricks file system

# first specify the schema for the dataset
# original dataset has column names with a '.' (Sepal.Length, Sepal.Width...) which are valid in R
# apparently, SparkSQL doesn't support names with embedded dots
schema <- structType(
  structField("id", "integer"),
  structField("Sepal_Length", "double"),
  structField("Sepal_Width", "double"),
  structField("Petal_Length", "double"),      
  structField("Petal_Width", "double"),
  structField("Species", "string"))
### structType()- Create a structType object that contains the metadata for a SparkDataFrame. Intended for use with createDataFrame and toDF.
### structField() - Create a structField object that contains the metadata for a single field in a schema.

iris_sparkDF <- read.df("dbfs:/databricks-datasets/Rdatasets/data-001/csv/datasets/iris.csv",
                   source = "csv", header="true", schema = schema)
### read.df() - Returns the dataset in a data source as a SparkDataFrame.
### The data source is specified by the 'source' and a set of options(...). 
### If 'source' is not specified, the default data source configured by "spark.sql.sources.default" will be used. 
### Similar to R read.csv, when 'source' is "csv", by default, a value of "NA" will be interpreted as NA.

persist(iris_sparkDF, "MEMORY_ONLY")
printSchema(iris_sparkDF)

# COMMAND ----------

head(iris_sparkDF)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Supervised Learning
# MAGIC Let's build a binomial generalized linear model to classify irises.

# COMMAND ----------

# let's build a model to distinguish 'versicolor' from 'virginica' irises based on features
# create training and testing sets of the iris dataset
# note that a proper sample will have all Species represented equally in the training set
# in theory, we could use `sampleBy()` to get a stratified sample without replacement
versicolor <- filter(iris_sparkDF, iris_sparkDF$Species == "versicolor")
versTraining_sparkDF <- sample(versicolor, FALSE, 0.6, 42)
versTesting_sparkDF <- except(versicolor, versTraining_sparkDF)

virginica  <- filter(iris_sparkDF, iris_sparkDF$Species == "virginica")
virgTraining_sparkDF <- sample(virginica, FALSE, 0.6, 43)
virgTesting_sparkDF <- except(virginica, virgTraining_sparkDF)

irisTraining_sparkDF <- rbind(versTraining_sparkDF, virgTraining_sparkDF)
irisTesting_sparkDF  <- rbind(versTesting_sparkDF, virgTesting_sparkDF)

### sample() - Return a sampled subset of this SparkDataFrame using a random seed.
### except() - Return a new SparkDataFrame containing rows in this SparkDataFrame but not in another SparkDataFrame. This is equivalent to 'EXCEPT' in SQL.

# COMMAND ----------

# fit a generalized linear model of family "binomial" with spark.glm
iris_glmM <- spark.glm(irisTraining_sparkDF, 
                       Species ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width, 
                       family = "binomial")
### spark.glm() - Fits generalized linear model against a Spark DataFrame. 
### Users can call summary to print a summary of the fitted model, predict to make predictions on new data, 
### and write.ml/read.ml to save/load fitted models.

# model summary
summary(iris_glmM)


# COMMAND ----------

# let's see how it did
irisTrain_pred <- predict(iris_glmM, irisTraining_sparkDF)
irisTrain_pred$class = ifelse (irisTrain_pred$prediction < 0.5, 0, 1)
display(irisTrain_pred)
# predict() creates a new SparkDataFrame with the original data and a "prediction" column
# "label" is the code the algorithm uses for the class

# COMMAND ----------

collect(count(groupBy(irisTrain_pred, irisTrain_pred$class, irisTrain_pred$label)))
# not bad

# COMMAND ----------

# but that was just for the training set, let's do predictions for the testing set
irisTest_pred <- predict(iris_glmM, irisTesting_sparkDF)
irisTest_pred$class = ifelse (irisTest_pred$prediction < 0.5, 0, 1)
collect(count(groupBy(irisTest_pred, irisTest_pred$class, irisTest_pred$label)))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Unsupervised Learning
# MAGIC Let's use the K-Means Clustering algorithm to group the irises based on their features.

# COMMAND ----------

# let's cluster irises based on their measured features with spark.kmeans
# remember that k-means can only use columns with numerical values
iris_kmM <- spark.kmeans(iris_sparkDF, 
                         ~ Sepal_Length + Sepal_Width + Petal_Length + Petal_Width, 
                         k = 3)
### Fits a k-means clustering model against a Spark DataFrame, similarly to R's kmeans(). 
### Users can call `summary()` to print a summary of the fitted model, `predict()` to make
### predictions on new data, and `write.ml()`/`read.ml()` to save/load fitted models.

# model summary
model_summ <- summary(iris_kmM)
model_summ
# coefficients are the centroids of the clusters
# note the size of the clusters; there are 50 data points per Species in the dataset
# cluster is a SparkDataFrame with the "prediction" corresponding to the assigned cluster 

# COMMAND ----------

# how do you get the cluster assigned to the datapoint?
# model summary has the cluster assignment
# REMEMBER: collect will bring nrow() numbers to the master
collect(model_summ$cluster)

# COMMAND ----------

# SparkR also provides a predict() method for a spark.kmeans model
# get fitted result from the k-means model
irisClusters <- predict(iris_kmM, iris_sparkDF)
### predict() - Makes predictions from a MLlib model
display(irisClusters)

# COMMAND ----------

# build contingency table to find the composition of the clusters
# count how many of each Species got into each of the clusters
# remember that Species was not used in the clustering algorithm
irisResults <- collect(count(groupBy(irisClusters, irisClusters$prediction, irisClusters$Species)))
irisResults

# COMMAND ----------

# MAGIC %md
# MAGIC ####User Defined Functions
# MAGIC 
# MAGIC In SparkR, there are separate functions depending on whether you want to run R code on each partition of a Spark DataFrame (`dapply`), or each group (`gapply`). With these functions you must supply the schema ahead of time. 

# COMMAND ----------

# MAGIC %md
# MAGIC #####dapply
# MAGIC 
# MAGIC Apply a function to each partition of a SparkDataFrame.

# COMMAND ----------

iris_spark_repart <- repartition(iris_sparkDF,10)
getNumPartitions(iris_spark_repart)

# COMMAND ----------

add_species_code <- function(name){
  if(name=='setosa'){
      return("SA")
    }else if(name=='virginica'){
      return("VA")
    }else if(name=='versicolor'){
      return("VR")
    }else{
      return("NA")
    }
}

#Define a user defined function to execute on each partition
compute_fn <- function(df){
  df$Code <- add_species_code(df$Species)
  return(df)
}
  

# COMMAND ----------

#TODO call dapply with appropriate data_frame and user defined function
return_schema_dapply <- structType(
  structField("id", "integer"),
  structField("Sepal_Length", "double"),
  structField("Sepal_Width", "double"),
  structField("Petal_Length", "double"),      
  structField("Petal_Width", "double"),
  structField("Species", "string"),
  structField("Code", "string"))

result_df <- 
  dapply(
    #iris_spark_repart, 
    #compute_fn, 
    return_schema_dapply 
  )

# COMMAND ----------

# MAGIC %md
# MAGIC You can use `dapplyCollect` to collect the result in a single operation

# COMMAND ----------

 dapplyCollect(
    iris_spark_repart, 
    compute_fn)

# COMMAND ----------

# MAGIC %md
# MAGIC #####gapply
# MAGIC 
# MAGIC Groups the SparkDataFrame using the specified columns and applies the R function to each group.

# COMMAND ----------

return_schema_gapply <- structType(
  structField("Species", "string"),
  structField("Sepal_Length", "double"),
  structField("Sepal_Width", "double"),
  structField("Petal_Length", "double"),      
  structField("Petal_Width", "double"))

#Calculate the mean of Sepal and Petal measurements for each Species
#TODO specify the result of the group aggregation
display(gapply(iris_sparkDF,
               "Species",
               function(key, x) {
                  #y <- data.frame(key, 
                  #                mean(x$Sepal_Length), 
                  #                mean(x$Sepal_Width),
                  #                mean(x$Petal_Length),
                  #                mean(x$Petal_Width), 
                  #                stringsAsFactors = FALSE)
               },
               return_schema_gapply
              )
        )

# COMMAND ----------

#Let us unpersist and release the memory
unpersist(iris_sparkDF)

# COMMAND ----------


