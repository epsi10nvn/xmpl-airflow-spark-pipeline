# read.py
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
		.config('spark.mongodb.input.uri', 'mongodb://mongo:27017/airflow_db.questions') \
		.config('spark.mongodb.input.uri', 'mongodb://mongo:27017/airflow_db.answers') \
        .getOrCreate()
    
    df = spark.read.csv("./include/data.csv", header="true")
    df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()