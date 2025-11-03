# Serverless ETL Pipeline using AWS Lambda and Glue

## Overview
This project demonstrates a serverless ETL pipeline built on AWS, where ingestion events from Amazon S3 trigger a Lambda function that orchestrates a downstream AWS Glue job implemented in PySpark. The pipeline is designed to process structured or semi‑structured data, transform it into a refined format and load it into a target data store.

The high‑level architecture is:

1. **S3 Event Trigger** – Data landing in a source bucket triggers an event.
2. **AWS Lambda** – The event invokes a Lambda function that starts a Glue ETL job and optionally refreshes a Glue crawler.
3. **AWS Glue Job** – A PySpark job runs inside Glue to read the raw data, perform transformations and write the results to a destination bucket in a curated “silver” layer.
4. **Data Catalog** – Optionally a Glue crawler can catalog new partitions for downstream analytics.

A diagram of this flow is included in `architecture.png`.

## Repository Contents
- **lambda_function.py** – A sanitized Lambda handler (Python) that listens for S3 events and starts a Glue job. It includes placeholders for your own job name, crawler name and target S3 paths.
- **pyspark_job.py** – A PySpark script (sanitized) that forms the core of the Glue job. It reads data from the raw bucket, applies transformations using Spark DataFrame APIs, and writes the refined data back to S3.
- **architecture.png** – Diagram illustrating the end‑to‑end ETL pipeline.
- **README.md** – Project description and usage instructions.

## Usage
To adapt this project to your environment:

1. **Prepare Resources**
   - Set up an S3 bucket to receive raw input data.
   - Create a Glue job with Python 3.x and attach necessary IAM policies.
   - Update `lambda_function.py` with your Glue job name, crawler name and S3 destinations.

2. **Deploy Lambda**
   - Zip `lambda_function.py` and upload it to a Lambda function configured with an S3 trigger pointing to your source bucket.
   - Ensure the Lambda role can start Glue jobs and crawl resources.

3. **Upload PySpark Job**
   - In the AWS Glue console, create a new job and upload `pyspark_job.py` as the script.
   - Configure the job’s IAM role, job parameters and output locations.

4. **Test the Pipeline**
   - Drop a sample file into the source bucket. The S3 event should trigger the Lambda function, which in turn starts the Glue job. Check CloudWatch logs for monitoring and confirm the refined data appears in the target location.

## Notes
- The provided code is sanitized; replace placeholder values (`<your_glue_job_name>`, `<your_target_s3_path>`, etc.) with your actual resource names.
- This repository is intended for demonstration and portfolio purposes. It assumes familiarity with AWS services such as Lambda, Glue, IAM and S3.
