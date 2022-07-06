# Databricks notebook source
# MAGIC %%sh
# MAGIC rm -rf /tmp/mleap_python_model_export
# MAGIC mkdir /tmp/mleap_python_model_export

# COMMAND ----------

#Remove Delta saved paths 
dbutils.fs.rm("/tmp/delta/employees", True)
dbutils.fs.rm("/tmp/delta/disasters", True)
dbutils.fs.rm("/tmp/delta/silver", True)
dbutils.fs.rm("/tmp/delta/silver", True)
dbutils.fs.rm("/tmp/delta/employee_gold", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS Demos CASCADE;
# MAGIC CREATE DATABASE Demos;
# MAGIC USE Demos;
# MAGIC -- Create the employee_bronze table.
# MAGIC CREATE TABLE employee_bronze USING DELTA LOCATION "/tmp/delta/employees";
# MAGIC -- Create the incident_bronze table.
# MAGIC CREATE TABLE incident_bronze USING DELTA LOCATION '/tmp/delta/disasters';
# MAGIC -- Create the employees_silver table.
# MAGIC CREATE TABLE employee_silver USING DELTA LOCATION '/tmp/delta/employees_silver';
# MAGIC -- Create the incident_silver table
# MAGIC CREATE TABLE incident_silver USING DELTA LOCATION '/tmp/delta/incidents_silver';
# MAGIC -- Create the employee_gold table
# MAGIC CREATE TABLE employee_gold USING DELTA LOCATION "/tmp/delta/employee_gold";
