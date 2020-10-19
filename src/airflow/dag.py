from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 14),
    'retries': 5,
    'schedule_interval': '@daily',
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ongo', default_args=default_args)

cmd_to_redshift = "spark-submit \
--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--driver-class-path RedshiftJDBC42-no-awssdk-1.2.45.1069.jar \
--master masterDNS:7077 \
s3toredshift.py"

pyspark_job = BashOperator(
                       task_id='s3 to redshift',
                       bash_command=cmd_to_redshift
                       dag=dag)

# setting the dependencies: run pyspark job after loading apps data into s3

#ios_s3 >> pyspark_job
#android_s3 >> pyspark_job
#tre_s3 >> pyspark_job
