# DataPipeline with Spark and MongoDB on Airflow Using Astro CLI and Docker

This project demonstrates the creation of a **data pipeline** using **Apache Airflow**, **Apache Spark**, and **MongoDB**. The pipeline leverages **Astro CLI** and **Docker** to run Apache Spark and MongoDB on Airflow, allowing for efficient data processing and analysis.

The data pipeline consists of several tasks including data downloading, importing to MongoDB, Spark processing, and result saving—all orchestrated by Airflow.

## Project Overview

The pipeline consists of the following tasks, orchestrated using Apache Airflow:

1. **Start and End**:

   - `DummyOperator` that define the start and end points of the pipeline.

2. **Branching**:

   - A `BranchPythonOperator` checks if the required files exist. If they do, the pipeline ends; if not, it continues to the next tasks.

3. **Clear File**:

   - A `BashOperator` clears existing files to ensure the environment is clean before downloading fresh data.

4. **Data Download**:

   - Two `PythonOperators` run in parallel to download and extract the question and answer datasets from Google Drive.

5. **Data Import to MongoDB**:

   - Two `PythonOperators` read the CSV files and import the data into MongoDB as separate collections.

6. **Spark Job Execution**:

   - A `SparkSubmitOperator` is used to execute a Spark job that joins the two MongoDB collections, counts the number of answers for each question, and saves the results to a new CSV file.

7. **Result Import to MongoDB**:
   - A final `PythonOperator` imports the results from the Spark job back into MongoDB.

This workflow runs locally using **Astro CLI** and **Docker** to manage the services and ensure a smooth execution of the pipeline.

## Data Details

- **Source**: The datasets used in this pipeline are from Kaggle, available at [StackSample: Stack Overflow Questions and Answers Dataset](https://www.kaggle.com/datasets/stackoverflow/stacksample/data).
- **Dataset Description**: This dataset contains the text of 10% of the questions and answers from the Stack Overflow programming Q&A website. The data is organized into three tables:

  1. **Questions**: Contains the title, body, creation date, closed date (if applicable), score, and owner ID for all non-deleted Stack Overflow questions whose ID is a multiple of 10.
  2. **Answers**: Contains the body, creation date, score, and owner ID for each answer, with a `ParentId` column linking back to the `Questions` table.
  3. **Tags**: Contains the tags for each question.

- **Pipeline Usage**: This pipeline uses only the **Questions** and **Answers** tables for processing. The workflow joins these tables to count the number of answers for each question.

- **Google Drive Setup**:

  1. Download the `Questions.csv` and `Answers.csv` files from the Kaggle dataset.
  2. Upload these files to **Google Drive** and ensure the sharing settings allow access.
  3. Extract the **file ID** from the shared link (e.g., `https://drive.google.com/file/d/<file_id>/view?usp=sharing`).

- **Data Download**: During pipeline execution, the files are downloaded from Google Drive using the file IDs, extracted, and prepared for further processing.

## Folder Structure

```
├── .astro/
├── apps/
├── dags/
│   └── google_drive_downloader
│   │   └── google_drive_downloader.py  #
│   └── datapipelines.py                # Airflow DAG definition
├── data/                               # Local data storage
├── include/
│   └── scripts
│       └── sparkSolve.py               # Spark job definition
├── docker-compose.override.yml         # Configuration for Spark and MongoDB
├── Dockerfile                          # Custom Dockerfile for Airflow
└── requirements.txt                    # Apache Airflow providers dependencies
```

## Prerequisites

1. Docker and Docker Compose installed.
2. Astro CLI installed.

## Setup Instructions

1. **Clone the repository**  
   Clone this repository to your local machine:

   ```bash
   git clone <repository_url>
   cd <repository_folder>
   ```

2. **Start the environment**  
   Initialize and start the Astro CLI environment:

   ```bash
   astro dev start
   ```

3. **Access the Airflow UI**  
   Open your browser and navigate to [http://localhost:8080](http://localhost:8080). Use the default credentials:

   - Username: `admin`
   - Password: `admin`

   ### Create Airflow Connections

   To enable MongoDB and Spark integrations, set up the following connections:

   - **MongoDB Connection**

     - Go to **Admin > Connections**.
     - Create a new connection with:
       - **Conn Id**: `mongo_default`
       - **Conn Type**: `MongoDB`
       - **Host**: `mongo`
       - **Port**: `27017`
       - **Schema**: `database_name` (e.g., `airflow_db`)
       - Leave other fields empty.

   - **Spark Connection**
     - Go to **Admin > Connections**.
     - Create a new connection with:
       - **Conn Id**: `spark_conn`
       - **Conn Type**: `Spark`
       - **Host**: `spark://spark-master`
       - **Port**: `7077`

![airflow connections setup](/src/images/connections.png)

4. **Run the DAG**  
   In the Airflow UI, locate the DAG named `spark_datapipeline`, manually trigger a run by clicking the play button.

5. **Verify Data in MongoDB**  
   After the pipeline completes, verify the data in MongoDB by executing the following commands in your terminal:

   ```bash
   docker exec -it <mongodb_container_name_or_id> /bin/bash
   ```

   Replace `<mongodb_container_name_or_id>` with either the **container name** (e.g., `airflow-pyspark_48735d-mongo-1`) or the **container ID**, which you can find by running:

   ```bash
   docker ps
   ```

   Once inside the Mongo shell, you can inspect the data:

   ![mongo_test](/src/images/mongosh.png)

## Configuration Details

### `docker-compose.override.yml`

This file defines the configuration for Spark master, Spark worker, and MongoDB services to run locally with Docker.

### `Dockerfile`

The Dockerfile sets up the environment with OpenJDK-17 and the necessary dependencies for Airflow and Spark.

### `requirements.txt`

Lists the required Python packages, including Airflow providers for Spark and MongoDB.

## DAG Workflow

![DAG](/src/images/dag.png)

## Learn More

For a detailed explanation of the project, including its design, implementation, and future improvements, click **[here](https://your-blog-link.com)**
