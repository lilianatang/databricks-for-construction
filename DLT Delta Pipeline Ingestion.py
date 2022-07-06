# Databricks notebook source
# MAGIC %md
# MAGIC ## ![](https://github.com/lilianatang/construction-databricks-demo/blob/main/DataPipelineFlow.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC # Build a Lakehouse to deliver value to your buiness lines

# COMMAND ----------

# Import necessary libraries
!pip install mleap
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, NaiveBayes
from pyspark.ml.feature import StringIndexer, Tokenizer, HashingTF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import when
from pyspark.sql.functions import lit

# COMMAND ----------

#Set up and declare global variables
dbutils.fs.unmount("mount_point")
dbutils.fs.mount(
  source = "source",
  mount_point = "mount_point",
  extra_configs = {"host":dbutils.secrets.get(scope = "scope", key = "key")})

employee_csv_path = "mount_point/employee-info"
write_format = 'delta'
train_csv_path = "mount_point/disaster_tweets_train"
test_csv_path = "mount_point/disaster_tweets_test"
serialized_path = "jar:file:/tmp/mleap_python_model_export/disaster_pipeline-json.zip"
dbfs_path = "dbfs:/dbfspath/disaster_pipeline-json.zip"

# COMMAND ----------

# MAGIC %md 
# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Bronze: loading data from blob storage

# COMMAND ----------

@dlt.table(
  comment = 'The raw structured employee dataset, ingested from Azure Blob storage'
)
def employee_structured_raw():
  return (spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(employee_csv_path))

@dlt.table(
  comment = 'The raw processing of unstructured data from superintendents daily reports, and storing raw predicted safety incident data in bronze table'
)
def safety_incidents_to_bronze():
  df_disaster_tweets_test = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(test_csv_path)
  df_disaster_tweets_train = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(train_csv_path)
  # remove null values
  df_disaster_tweets_train = df_disaster_tweets_train.filter(df_disaster_tweets_train.target.isNotNull())
  df_disaster_tweets_train = df_disaster_tweets_train.filter(df_disaster_tweets_train.text.isNotNull())
  # Define ML pipeline
  labelIndexer = StringIndexer(inputCol="target", outputCol="label", handleInvalid="keep")
  tokenizer = Tokenizer(inputCol="text", outputCol="words")
  hashingTF = HashingTF(inputCol="words", outputCol="features")
  dt = NaiveBayes()
  pipeline = Pipeline(stages=[labelIndexer, tokenizer, hashingTF, dt])
  # Tune ML pipeline
  paramGrid = ParamGridBuilder().addGrid(hashingTF.numFeatures, [1000, 2000]).build()
  cv = CrossValidator(estimator=pipeline, evaluator=MulticlassClassificationEvaluator(), estimatorParamMaps=paramGrid)
  cvModel = cv.fit(df_disaster_tweets_train)
  model = cvModel.bestModel
  sparkTransformed = model.transform(df_disaster_tweets_train)
  # Load Trained Model to Make Predictions
  df_disaster_tweets_test = df_disaster_tweets_test.filter(df_disaster_tweets_test.text.isNotNull())
  predictions = model.transform(df_disaster_tweets_test)
  # Define the output format, path and the table name.
  save_path = '/tmp/delta/disasters'
  # Write the data to its target.
  predictions.write \
    .option("mergeSchema", "true") \
    .format(write_format) \
    .mode("append") \
    .save(save_path)
  return predictions

@dlt.table(
  comment = 'Storing raw employee data in bronze table'
)
def employee_to_bronze():
  df_employees = dlt.read("employee_structured_raw")
  # Define the output format, path and the table name.
  save_path = '/tmp/delta/employees'
  # Write the data to its target.
  df_employees.write \
    .option("mergeSchema", "true") \
    .format(write_format) \
    .mode("append") \
    .save(save_path)
  return df_employees
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Silver data: anonymized table, date cleaned
# MAGIC 
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC 
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

@dlt.table(
  comment = 'Cleaning employee data and storing them in a silver table'
)
def employee_to_silver():
  employees = dlt.read("employee_to_bronze")
  employees = employees.select([c for c in employees.columns if c in ['Age','BusinessTravel','Department', 'EmployeeNumber', 'MonthlyIncome']])
  employees = employees.withColumn("location", \
      when((employees.MonthlyIncome < 4000), lit("Calgary")) \
     .when((employees.MonthlyIncome >= 4000) & (employees.MonthlyIncome <= 5000), lit("Edmonton")) \
     .otherwise(lit("Vancouver")) \
  )
  # Perform aggegration on HeadCount by location
  employees = employees.groupBy("location").count()
  # rename count column
  employees = employees.withColumnRenamed("count","headcount")
  # Define the output format, path and the table name.
  save_path = '/tmp/delta/employees_silver'

  # Write the data to its target.
  employees.write \
    .option("mergeSchema", "true") \
    .format(write_format) \
    .mode("append") \
    .save(save_path)
  return employees

@dlt.table(
  comment = 'Cleaning safety incidents data and storing them in a silver table'
)
def safety_incidents_to_silver():
  incidents = dlt.read("safety_incidents_to_bronze")
  incidents = incidents.select([c for c in incidents.columns if c in ['text','location','prediction']])
  incidents = incidents.withColumn("location", \
      when((incidents.location.isNull())  & (incidents.prediction == 0), lit("Calgary")) \
     .when((incidents.location.isNull()) & (incidents.prediction == 1), lit("Edmonton")) \
     .otherwise(incidents.location) \
  )
  # Perform aggegration on Number of Report of Incidents by location
  incidents = incidents.filter(incidents.prediction ==1).groupBy("location").count()
  # rename count column
  incidents = incidents.withColumnRenamed("count","number_of_predicted_incidents")  
  # Define the output format, path and the table name.
  save_path = '/tmp/delta/incidents_silver'
  # Write the data to its target.
  incidents.write \
    .option("mergeSchema", "true") \
    .format(write_format) \
    .mode("append") \
    .save(save_path)
  return incidents

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Gold table: Employees joined with their Location's Incidents
# MAGIC 
# MAGIC We can now join the 2 tables based on the location to create our final gold table.

# COMMAND ----------

@dlt.table(
  comment = 'Joining employees and safety incidents cleaned datasets by location to form an employee gold table'
)
def employee_gold_table():
  save_path = '/tmp/delta/employee_gold'
  incidents = dlt.read("safety_incidents_to_silver")
  golden_records = dlt.read("employee_to_silver").join(incidents, "location", "right")
  (golden_records
     .write.format('delta') \
     .option("mergeSchema", "true") \
     .mode("append") \
     .save(save_path))
  return golden_records
