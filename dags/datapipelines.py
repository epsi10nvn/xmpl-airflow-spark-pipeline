from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from google_drive_downloader.google_drive_downloader import GoogleDriveDownloader as gdd

from pyspark import SparkContext
from pyspark.sql import SparkSession

from datetime import datetime
import csv
import pandas as pd
import os
import glob
# Path để kiểm tra file
QUESTION_FILE_PATH = "./data/Question.zip"
ANSWER_FILE_PATH = "./data/Answers.zip"

def _check_exists_files():
    if os.path.exists(QUESTION_FILE_PATH) and os.path.exists(ANSWER_FILE_PATH):
        # return "end"
        return "clear_file"
    return "clear_file"

# https://drive.google.com/file/d/1Db0s3CmgIQVP8XMJ2aD6RMRrKUtdfPrt/view?usp=sharing
# https://drive.google.com/file/d/1ekifz8-vhU7eladrW3nzB5bu2Aj97yR8/view?usp=sharing
QUESTION_FILE_ID = "1Db0s3CmgIQVP8XMJ2aD6RMRrKUtdfPrt"
ANSWER_FILE_ID = "1ekifz8-vhU7eladrW3nzB5bu2Aj97yR8"

def _download_file(file_id, destination_path):
    gdd.download_file_from_google_drive(
        file_id=file_id,
        dest_path=destination_path,
        unzip=True
    )

def _insert_csv_to_mongo(file_path, collection):
    try:
        mongo_hook = MongoHook(conn_id='mongo_default')
        collection = mongo_hook.get_collection(collection)
        chunk_size = 10000  # Process 10,000 rows at a time

        for chunk in pd.read_csv(file_path, chunksize=chunk_size, encoding="latin1"):
            # Convert each chunk to a list of dictionaries
            data = chunk.to_dict(orient='records')
            # Insert into MongoDB
            if data:
                collection.insert_many(data)
                print(f"Inserted {len(data)} records")
                
    except Exception as e:  
        print("An error occurred:", e) 

def _push_xcom_file_path(context):
    ti = context['task_instance']
    output_dir = '/usr/local/airflow/output/'  
    file_list = glob.glob(f"{output_dir}part-*.csv")  
    if file_list:  
        ti.xcom_push(key='output_file', value=file_list[0])  # Push the first file found  
    else:  
        raise FileNotFoundError("No CSV file found in output directory.")  

with DAG(
    "spark_datapipeline", 
    start_date=datetime(2024, 1, 1), 
    schedule_interval='@daily', 
    catchup=False
) as dag:
    
    # Task: start và end
    start = DummyOperator(
        task_id='start'
    )
    end = DummyOperator(
        task_id='end'
    )
    
    # Task: branching
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=_check_exists_files    
    )
    
    # Task: clear_file
    clear_file = BashOperator(
        task_id="clear_file",
        bash_command=f"rm -f {QUESTION_FILE_PATH} {ANSWER_FILE_PATH}"
    )
    
    # Task: download_question_file_task
    download_question_file_task = PythonOperator(
        task_id="download_question_file_task",
        python_callable=_download_file,
        op_kwargs={
            "file_id": QUESTION_ID,
            "destination_path": QUESTION_FILE_PATH
        }
    )
    
    # Task: download_answer_file_task
    download_answer_file_task = PythonOperator(
        task_id="download_answer_file_task",
        python_callable=_download_file,
        op_kwargs={
            "file_id": ANSWER_ID,
            "destination_path": ANSWER_FILE_PATH
        }
    )
    
    # Task: import questions to mongo
    import_questions_mongo = PythonOperator(
        task_id="import_questions_mongo",
        python_callable=_insert_csv_to_mongo,
        op_kwargs={
            "file_path": './data/Dataset/Questions.csv',
            "collection": 'questions'
        }
    )
    
    # Task: import answers to mongo
    import_answers_mongo = PythonOperator(
        task_id="import_answers_mongo",
        python_callable=_insert_csv_to_mongo,
        op_kwargs={
            "file_path": './data/Dataset/Answers.csv',
            "collection": 'answers'
        }
    )
    
    # Task: Spark
    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id="spark_conn",
        application="include/scripts/sparkSolve.py",
        packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.2",
        verbose=True,
        on_success_callback=_push_xcom_file_path,
        do_xcom_push=True
    )
    
    # Task: import output to mongo
    import_output_mongo = PythonOperator(
        task_id="import_output_mongo",
        python_callable=_insert_csv_to_mongo,
        op_kwargs={
            "file_path": "{{ task_instance.xcom_pull(task_ids='submit_job', key='output_file') }}",
            "collection": "questions_answers_stats"
        }
    )
    
    start >> branching >> [clear_file, end]
    clear_file >> [download_question_file_task, download_answer_file_task]
    download_question_file_task >> import_questions_mongo
    download_answer_file_task >> import_answers_mongo
    [import_questions_mongo, import_answers_mongo] >> submit_job >> import_output_mongo >> end
    