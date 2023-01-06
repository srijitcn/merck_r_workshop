# Databricks notebook source
# MAGIC %md
# MAGIC ####SparklyR
# MAGIC 
# MAGIC [SparklyR](https://spark.rstudio.com/) is an R interface to Spark, developed by RStudio. It lets you run distributed code in Spark and give access to Spark’s distributed Machine Learning libraries, Structure Streaming,and ML Pipelines from R.
# MAGIC 
# MAGIC In this exercise we will
# MAGIC - Create some dummy dataset and add a new column using Spark
# MAGIC - Persist the data to a delta table
# MAGIC - Read the data into Spark
# MAGIC - Create a linear regression model using Spark and track using MLFlow
# MAGIC - Perform a kmeans clustering in Spark
# MAGIC - Demonstrate the parallelization of multiple executions of non-parallelizable process
# MAGIC 
# MAGIC This [e-book](https://therinspark.com/) provides a detailed explanation of SparklyR usages and optimizations

# COMMAND ----------

#TODO Replace EEE with your email
root_dbfs <- "/dbfs/rworkshop"
user_email <- EEE

root_user_dbfs <- paste0(root_dbfs,"/",user_email)
database_directory <- paste0(root_user_dbfs,"/data")
mlflow_directory <- paste0(root_user_dbfs,"/mlflow")
files_directory <-  paste0(root_user_dbfs,"/files")

print(paste0(" Database directory: ", database_directory))
print(paste0(" MLFlow directory: ", mlflow_directory))
print(paste0(" Files directory: ", files_directory))


dbutils.fs.rm(database_directory, TRUE)
dbutils.fs.rm(mlflow_directory,TRUE)

dbutils.fs.mkdirs(database_directory)
dbutils.fs.mkdirs(mlflow_directory)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prepare Data
# MAGIC We will use Iris dataset for this workshop

# COMMAND ----------

library(dplyr)
head(iris)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In real applications, your data is usually very big and cannot fit into a hard disk or memory of a single node and it is very likely your data is already in Delta tables. You can use `SparkDataFrame` to analyze your data in Spark system directly. Here, we illustrate how to copy a local dataset to Spark environment and then work on that dataset in the Spark system. As we have already created the Spark Connection sc, it is fairly simple to copy data to spark system by `sdf_copy_to()` function as below:

# COMMAND ----------

class(iris)

# COMMAND ----------

library(sparklyr)

#create a spark connection
sc <- spark_connect(method = "databricks")

#use `copy_to` method of SparklyR to copy the r dataframe to Spark
iris_spark <- sdf_copy_to(sc, iris, "sparklyr_vars", overwrite = TRUE, repartition=5)


# COMMAND ----------

# MAGIC %md
# MAGIC The above one line code copies iris dataset from local node to Spark cluster environment where sc is the Spark Connection we just created.
# MAGIC 
# MAGIC The `overwrite` option specifies whether we want to overwrite the target object if the same name `SparkDataFrame` exists in the Spark environment. Finally `sdf_copy_to()` function will return an R object wrapping the copied `SparkDataFrame`. So `iris_spark` can be used to refer to the iris `SparkDataFrame`.

# COMMAND ----------

head(iris_spark)

# COMMAND ----------

class(iris_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data Analysis using SparklyR
# MAGIC 
# MAGIC Now let us do some data analysis on the spark dataframe we created. 
# MAGIC 
# MAGIC With the sparklyr packages, we can use many functions in `dplyr` to SparkDataFrame directly 

# COMMAND ----------

iris_spark %>% count

# COMMAND ----------

iris_spark %>% arrange(species)

# COMMAND ----------

# bit more complex manipulation
iris_spark %>% 
  mutate(Sepal_Width_Rnd = ROUND(Sepal_Width * 2) / 2) %>% # Bucketizing Sepal_Width
  group_by(Species) %>% 
  summarize(count = n(), Sepal_Width_Rnd_Avg = mean(Sepal_Width_Rnd), Sepal_Length_Avg = mean(Sepal_Length), Sepal_Length_Std = sd(Sepal_Length)) 

# COMMAND ----------

# MAGIC %md
# MAGIC We can register this dataframe as a temporary table and can run SQL commands, if needed

# COMMAND ----------

#TODO Fill out XXX with temp table name. Add your initials to avoid conflict eg: 'iris_train_temp_YOUR INITIALS'
temp_table_name = XXX

sdf_register(iris_spark, temp_table_name)

sdf_sql(sc,paste0("SELECT * FROM ",temp_table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC --we can also use SQL 
# MAGIC --replace XXX with the correct name
# MAGIC SELECT * FROM XXX;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Persisting Data into Delta

# COMMAND ----------

# MAGIC %md
# MAGIC #####Option 1
# MAGIC There are many ways to persist this table as Delta tables. Let us write it out as an external table to a dbfs location.

# COMMAND ----------

#TODO Fill out XXX with table name Add your initials to avoid conflict eg: 'iris_train_YOUR INITIALS'

iris_delta_table_name <- XXX

iris_table_location <- paste0("dbfs:",database_directory, "/",iris_delta_table_name)

print(paste0("Location for table ",iris_delta_table_name," is: ",iris_table_location))

# COMMAND ----------

# MAGIC %md
# MAGIC If everything looks good, let us write the data to the location

# COMMAND ----------

spark_write_delta(iris_spark, path= iris_table_location)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can create an external table in the metastore with this data, so that everyone can use it

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC USE DATABASE rworkshop;

# COMMAND ----------

sdf_sql(sc, paste0("DROP TABLE IF EXISTS ",iris_delta_table_name))

# COMMAND ----------

sdf_sql(sc, paste0("CREATE TABLE IF NOT EXISTS ",iris_delta_table_name," USING delta LOCATION '",iris_table_location,"'"))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see the table in data explorer
# MAGIC 
# MAGIC Now we can query the data from anywhere

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO Replace XXX with the table name
# MAGIC SELECT * FROM XXX

# COMMAND ----------

# MAGIC %md
# MAGIC #####Option 2
# MAGIC Another way to create a managed table in metastore is directly querying the temporary table and writing to a new table with CTAS statement

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO Replace XXX with a new table name Add your initials to avoid conflict eg: 'iris_train_ctas_YOUR INITIALS'
# MAGIC --TODO Replace YYY with the temporary table name
# MAGIC CREATE TABLE XXX AS
# MAGIC SELECT * FROM YYY

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO Replace XXX with the new table name
# MAGIC SELECT * FROM XXX

# COMMAND ----------

# MAGIC %md
# MAGIC ####ML with SparklyR

# COMMAND ----------

# MAGIC %md
# MAGIC #####Supervised Learning
# MAGIC 
# MAGIC Let us use the Random Forest implementation in Spark to perform a multiclass classification on Iris data

# COMMAND ----------

#lets create train and test dataset
partitions <- iris_spark %>%
  sdf_random_split(training = 0.75, test = 0.25, seed = 1099)

# COMMAND ----------

rf_model <- partitions$training %>%
  ml_random_forest(Species ~ Petal_Length + Petal_Width, type = "classification")

# COMMAND ----------

rf_predict <- ml_predict(rf_model, partitions$test) %>%
  ft_string_indexer("Species", "Species_idx") %>%
  collect

table(rf_predict$Species_idx, rf_predict$prediction)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Unsupervised Learning

# COMMAND ----------

# MAGIC %md
# MAGIC Now let us run a K-Means Clustering algorithm from SparkML on this dataset

# COMMAND ----------

km <- ml_kmeans(iris_spark, k=3, features = c("Petal_Length", "Petal_Width"))

# COMMAND ----------

km

# COMMAND ----------

#Let us visualize the clusters
library(ggplot2)

ml_predict(km) %>%
  collect() %>%
  ggplot(aes(Petal_Length, Petal_Width)) +
  geom_point(aes(Petal_Width, Petal_Length, col = factor(prediction + 1)),
             size = 2, alpha = 0.5) + 
  geom_point(data = km$centers, aes(Petal_Width, Petal_Length),
             col = scales::muted(c("red", "green", "blue")),
             pch = 'x', size = 12) +
  scale_color_discrete(name = "Predicted Cluster",
                       labels = paste("Cluster", 1:3)) +
  labs(
    x = "Petal Length",
    y = "Petal Width",
    title = "K-Means Clustering",
    subtitle = "Use Spark.ML to predict cluster membership with the iris dataset."
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ###MLFlow
# MAGIC [MLflow](https://docs.databricks.com/mlflow/index.html) is an open source platform to manage the ML lifecycle, including experimentation, reproducibility, deployment, and a central model registry. 
# MAGIC It has the following primary components:
# MAGIC - **Models**: Allow you to manage and deploy models from a variety of ML libraries to a variety of model serving and inference platforms.
# MAGIC - **Projects**: Allow you to package ML code in a reusable, reproducible form to share with other data scientists or transfer to production.
# MAGIC - **Model Registry**: Allows you to centralize a model store for managing models’ full lifecycle stage transitions: from staging to production, with capabilities for versioning and annotating.
# MAGIC - **Model Serving**: Allows you to host MLflow Models as REST endpoints.

# COMMAND ----------

library(mlflow)

# COMMAND ----------

experiment_name = paste0("/Users/", user_email,"/mlflow_experiments/sparklyr_demo")

#Create an experiment or use an existing one
mlflow_exp_id = tryCatch(
  mlflow_create_experiment(experiment_name),
  error = function(e){
    return(mlflow_get_experiment(name=experiment_name)$experiment_id)
  }
)

# COMMAND ----------

mlflow_exp_id

# COMMAND ----------

library(carrier)

with(mlflow_start_run(experiment_id=mlflow_exp_id), {
  # Set the model parameters
  fit_intercept <- TRUE
  
  # Create and train model
  iris_lm <- ml_linear_regression(iris_spark, Petal_Length ~ Petal_Width, fit_intercept = fit_intercept) 
  
  # Log the model parameters used for this run
  mlflow_log_param("fit_intercept", fit_intercept)
  
  # Log the value of the metrics 
  mlflow_log_metric("mse", iris_lm$model$summary$mean_squared_error)
  
  # Log the model
  # The crate() function from the R package "carrier" stores the model as a function
  predictor <- carrier::crate(function(x) predict(iris_lm, .x))
  mlflow_log_model(predictor, "model")     
})

# COMMAND ----------

mlflow_end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Parallelization of multiple executions of non-parallelizable process
# MAGIC We can take advantage of distributed processing using `foreach.. %dopar%..` semantic. The foreach package provides the ``%dopar%`` operator to iterate over elements in a collection in parallel. Using Sparklyr, you can now register Spark as a backend using `registerDoSpark()` and then easily iterate over R objects using Spark

# COMMAND ----------

# MAGIC %md
# MAGIC The below example demonstrates a usecase where the requirement is to run simulations at scale. Thousands of iterations of simulation run usually takes hours to run on even multi core machines. 
# MAGIC 
# MAGIC These simulations are completely independent and therefore we can take advantage of the `foreach.. %dopar%..` semantic and execute them on a Spark cluster parallely thus reducing the total run time. 
# MAGIC 
# MAGIC Multiple such simulation workloads can be also run parallely using workflows, giving extreme parallelism

# COMMAND ----------

library(survival)

simFunc <- function(seed=12345,
                    N = 750,
                    rand.factor = 0.5,
                    median.st.com = 4.4,
                    median.st.exp = 6.28 ,
                    ov.drop.rate = 0.16,
                    drop.phase = c(3,6,12),
                    drop.dist  = c(0.5,0.3,0.15),
                    drop.perc.com = 0.5,
                    dbl = 18){
  if(!is.null(seed)) set.seed(seed)
  nc <- rbinom(1,size=N,prob=rand.factor)
  ne <- N-nc
  l.com <- log(2)/median.st.com
  l.exp <- log(2)/median.st.exp
  full.st.com <- rexp(n=nc,rate=l.com)
  full.st.exp <- rexp(n=ne,rate=l.exp)
  sim.data <- data.frame(USUBJID=paste0("ID_",1:N),
                         treatment=c(rep("Placebo",nc),rep("DrugXX",ne)),
                         full.time=c(full.st.com,full.st.exp))
  sim.data$treatment <- factor(sim.data$treatment,levels=c("Placebo","DrugXX"))
  sim.data$event <- ifelse(sim.data$full.time>dbl,0,1)
  sim.data$time <- ifelse(sim.data$event==1,sim.data$full.time,dbl)
  
  # drop.out simulation scheme 1
  n.drop <- ceiling(N*ov.drop.rate)
  drop.time <- lapply(1:length(drop.phase),function(i){
    runif(n=ceiling(n.drop*drop.dist[i]),
          min=ifelse(i==1,0,drop.phase[i-1]),
          max=ifelse(i==length(drop.phase),dbl,drop.phase[i]))
  })
  drop.time <- sample(unlist(drop.time),replace=F) # random pertubation of element seq
  drop.ID <- sapply(drop.time,function(xx){
    candidates <- which(sim.data$full.time>xx)
    cand.arm <- sample(c("Placebo","DrugXX"),size=1,prob=c(drop.perc.com,1-drop.perc.com))
    candidates <- candidates[sim.data$treatment[candidates]==cand.arm]
    if(length(candidates)>0){
      sample(candidates,size=1)
    }else{NA}
  }) # sample for drop out, only drop out time 
  drop.ID <- na.omit(drop.ID)
  
  sim.data$event[drop.ID] <- 0
  sim.data$time[drop.ID] <- drop.time
  sim.data$drop.out <- FALSE
  sim.data$drop.out[drop.ID] <- TRUE
  sim.data
}

analysisFunc <- function(use.data,dbl=18){
  out <- list()
  kmobj <- survfit(Surv(time,event)~treatment,use.data)
  lrobj <- survdiff(Surv(time,event)~treatment,use.data)
  
  out$logrank.pval <- 1-pchisq(lrobj$chisq,df=1)
  out$logrank.chisq <- lrobj$chisq
  out$desc <- as.data.frame(summary(kmobj)$table[,-c(2,3)])
  out$desc$n.drop <- tapply(use.data$drop.out,use.data$treatment,sum)
  out$desc$n.admin.censor <- out$desc$records - out$desc$events - out$desc$n.drop
  out$desc$dbl <- dbl
  
  out$desc.drug <- out$desc[2,,drop=F]
  out$desc.placebo <- out$desc[1,,drop=F]
  out <- out[-which(names(out)=="desc")]
  
  coxobj <- coxph(Surv(time,event)~treatment,use.data,robust=T)
  ana.result <- summary(coxobj)
  out$cox <- as.data.frame(ana.result$conf.int)
  
  out
}

# COMMAND ----------

library(foreach)

#register a spark backend for foreach
registerDoSpark(sc)

#specify the number of simulations
#num_sim = 75000

#reducing the number of iterations for the workshop, so that cluster is not overloaded
num_sim = 100

# COMMAND ----------

start_time <- Sys.time()
print(start_time)
useDbl <- 7

result <- foreach(i = 1:num_sim, .combine = 'rbind') %dopar% {
  analysisFunc(
    use.data = simFunc(seed = NULL, dbl = useDbl),
    dbl = useDbl
  )
}

print(paste0("Run time (minutes):", (as.numeric(Sys.time() - start_time , units = "mins"))))
