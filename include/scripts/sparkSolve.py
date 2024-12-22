from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import * 

import os

def createSparkSession():
    spark = SparkSession \
		.builder \
		.master('local[4]') \
		.appName('Assign2') \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
		.config('spark.mongodb.input.uri', 'mongodb://mongo:27017/airflow_db.questions') \
		.config('spark.mongodb.input.uri', 'mongodb://mongo:27017/airflow_db.answers') \
		.getOrCreate()
	
    return spark

def defineSchemas():
    ques_schema = StructType([
        StructField("Body", StringType(), True),
        StructField("ClosedDate", StringType(), True),
        StructField("CreationDate", StringType(), True),
        StructField("Id", IntegerType(), True),
        StructField("OwnerUserId", StringType(), True),
        StructField("Score", IntegerType(), True),
        StructField("Title", StringType(), True)
    ])
    
    ans_schema = StructType([
        StructField("Body", StringType(), True),
        StructField("CreationDate", StringType(), True),
        StructField("Id", IntegerType(), True),
        StructField("OwnerUserId", StringType(), True),
        StructField("ParentId", IntegerType(), True),
        StructField("Score", IntegerType(), True)
    ])
    return ques_schema, ans_schema

def write_to_csv(ques_stats_df):
  output_dir = "/usr/local/airflow/output"
  ques_stats_df.coalesce(1) \
  	.write \
		.mode("overwrite") \
    .option("header", "true") \
		.csv(output_dir)
  output_path = None
  for filename in os.listdir(output_dir):  
    if filename.startswith("part-") and filename.endswith(".csv"):
      output_path = filename
      break
  
  return output_path

def main():
  spark = createSparkSession()
  ques_schema, ans_schema = defineSchemas()
		
  ques_df = spark.read \
		.format('mongo') \
		.option('uri', 'mongodb://mongo/airflow_db.questions') \
		.schema(ques_schema) \
    .load()
	
  ques_df = ques_df \
     	.withColumn("ClosedDate", f.to_date(f.col("ClosedDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("CreationDate", f.to_date(f.col("CreationDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
	
  ans_df = spark.read \
		.format("mongo") \
		.option('uri', 'mongodb://mongo/airflow_db.answers') \
		.schema(ans_schema) \
    .load()
  
  ans_df = ans_df \
        .withColumn("CreationDate", f.to_date(f.col("CreationDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
	# ans_df_repart = ans_df.repartition(4)
  cleaned_ques_df = ques_df.withColumn(
		"OwnerUserId",
		f.when(f.col("OwnerUserId") == "NA", None).otherwise(f.col("OwnerUserId")).cast(IntegerType())
	).filter(f.col("OwnerUserId").isNotNull())

  cleaned_ans_df = ans_df.withColumn(
		"OwnerUserId",
		f.when(f.col("OwnerUserId") == "NA", None).otherwise(f.col("OwnerUserId")).cast(IntegerType())
	).filter(f.col("OwnerUserId").isNotNull())
	# ans_df.printSchema()

  join_expr = cleaned_ques_df.Id == cleaned_ans_df.ParentId
  cleaned_ans_renamed_df = cleaned_ans_df.withColumnRenamed("Id", "IdAns") \
		.withColumnRenamed("OwnerUserId", "OwnerUserIdAns") \
		.withColumnRenamed("Score", "ScoreAns") \
		.withColumnRenamed("CreationDate", "CreationDateAns")
  joined_df = cleaned_ques_df.join(cleaned_ans_renamed_df, join_expr, "inner") \
		.select("Id", "IdAns") \
		.groupBy("Id") \
		.agg(f.count("IdAns").alias("CountAns")) \
		.orderBy(f.col("Id"))
  
  output_path = write_to_csv(joined_df)
  print(f"Data written to {output_path}")
  joined_df.show()

  spark.stop()
  return output_path

if __name__ == "__main__":
  main()