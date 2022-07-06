# Lakehouse for the Construction Industry
### Demo Scenario
A construction company has experienced a high turnover rate due to the inability to understand employee trends and timely responses to incidents at job sites .

The business has determined that a focus must be placed on highly strategic employee retention while responding and following up on job site incidents within 48 hours. 

A consolidated Human Resources dashboard must be created to track employee journey, including age, education, professional development activities, etc (from the ERP system); to send Safety Advisors to job sites that experienced injuries (from superintendents’ daily reports) , to reduce turnover rate and ultimately minimize the impact of workplace injuries.
### Data Flow Architecture
![DataFlow](https://github.com/lilianatang/construction-databricks-demo/blob/main/DataPipelineFlow.png?raw=true)

### Code Explanation
The main data processing code is stored in ```https://github.com/lilianatang/construction-databricks-demo/blob/main/DLT%20Delta%20Pipeline%20Ingestion.py``` where we accomplished the following:
* Read Employee (structured) data out of Azure Blob Storage
* Read Superintendents' Daily Reports (unstructured data) out of Azure Blob Storage
* Process Unstructured Data (e.g. tokenization), train, and fine-tune a machine learning model to predict if an incident likely happenned at the job site based on the daily reports written by superintendents.
* Store raw employee data in ```employee_bronze(Age, Attrition, BusinessTravel, DailyRate, Department...etc)``` Delta table.
* Store raw predicted safety incidents in ```incident_bronze(text, location, prediction...etc)``` Delta table.
* Clean up and perform aggegration on raw employee data and store the processed data in ```employee_silver(location, headcount)``` Delta table
* Clean up and perform aggegration on raw predicted safety incidents data and store the processed data in ```incident_silver(location, number_of_predicted_incidents)``` Delta table
* Perform a RIGHT JOIN operation on ```employee_silver``` and ```incident_silver``` tables based on ```location``` to get golden records stored in ```employee_gold(location, headcount, number_of_predicted_incidents)``` for further descriptive analytics.
![HumanResourcesDashboard](https://github.com/lilianatang/construction-databricks-demo/blob/main/HRDashboard.png?raw=true)

### Why This Solution Saves Your Data Team So Much Time
In this demo, we accomplished a lot (reading two different datasets out of Azure Blob storage, processing both unstructured and structured data, trained and tuned a machine learning model on text data to predict if an incident likely happenned at a job site, cleaning up data, performing aggregation on cleaned datasets, and creating golden records in ```employee_gold``` table) within only 100 lines of codes because we can leverage fully managed data science tools, such as MLFlow, optimized Apache Spark, and Delta Lake within Databricks. All your data team (data engineer, data analyst, and data scientist) can collaborate in one single unified platform without having to spin up a seperate data lake (for Machine Learning works) in addition to a data warehouse (for building dashboards and reports).  