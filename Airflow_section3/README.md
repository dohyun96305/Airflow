# Airflow Section3
* Before add Airflow Sensor or Operator In DAG, Check **Airflow Documentation**

    https://airflow.apache.org/docs/apache-airflow/stable/operators-and-hooks-ref.html

* **Forex_Data_Pipeline**
  * Check Http URL and File by Sensor Operator
  * Preprocess Json file and Move file to HDFS
  * Create Hive Table and import data by execute HQL 
  * Send notification to Email, Slack
  * Make dependencies between tasks in DAG

* **In This Section**
  * Initiate DAG => 01
  * Add HttpSensor, FileSensor => 02, 03
  * Add PythonOperator, BashOperator => 04, 05, 
  * Add HiveOperator, SparkSubmitOperator => 06, 07
  * Add EmailOperator, SlackWebhookOperator => 08
  * Set Dependencies => 09
