"""
Lambda Function for S3 Metadata File Ingestion, Audit Lookup, Main File Presence Check,
Glue Job Trigger, and SNS Alerting

Overview:
---------
This Lambda function is triggered when a metadata file is uploaded with prefix
'LandingZone/SC360metadata' in the S3 bucket. On trigger:

1. Extracts main filename from metadata file name.
2. Queries audit_config table in RDS for allowed extension for the base filename.
3. Checks if the main file with required extension exists in S3.
4. If missing, sends SNS notification and exits.
5. If present, triggers Glue job for processing.
6. Logs all steps for traceability and error diagnosis.

Environment Variables Required:
- RDS_SECRET_NAME: Name of AWS Secrets Manager secret holding RDS credentials.
- GLUE_JOB_NAME: Name of the Glue job to trigger.
- SNS_ARN: ARN of the SNS topic for notifications.
- AWS_REGION (optional): AWS region.
"""

import os
import logging
import boto3
import psycopg2
import re
import ast
import json
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_connection(secret_name):
    """
    Retrieves RDS database credentials from AWS Secrets Manager and establishes a connection.

    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager containing RDS credentials.

    Returns:
        psycopg2.extensions.connection: Database connection object.

    Raises:
        KeyError: If any required credential is missing.
        Exception: If unable to retrieve credentials or connect to the database.
    """
    region_name = os.environ.get("AWS_REGION", "us-east-1")
    secrets_client = boto3.client('secretsmanager', region_name=region_name)

    try:
        logger.info(f"Fetching RDS credentials from Secrets Manager: {secret_name}")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        print("Creds Response:", response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(f"Successfully retrieved credentials for {secret_name}")
            creds = ast.literal_eval(response['SecretString'])
            host = creds.get("host")
            port = creds.get("port")
            dbname = creds.get("engine")
            username = creds.get("username")
            password = creds.get("password")
            if not all([host, port, dbname, username, password]):
                logger.error(f"Missing one or more required RDS credentials in {secret_name}")
                raise KeyError(f"Missing required RDS database credentials in {secret_name}")
            conn_string = f"dbname='{dbname}' port='{port}' user='{username}' password='{password}' host='{host}'"
            logger.info("Establishing database connection to RDS...")
            connection = psycopg2.connect(conn_string)
            logger.info("Database connection successful.")
            return connection
        else:
            logger.error(f"Failed to retrieve credentials for {secret_name}")
            raise RuntimeError(f"Failed to retrieve credentials for {secret_name}")
    except Exception as e:
        logger.error(f"Error getting RDS DB connection: {e}")
        raise

def extract_main_filename_from_metadata(metadata_filename):
    """
    Extracts main filename (without extension) from metadata filename.

    Args:
        metadata_filename (str): Metadata file name (SC360metadata_<main_filename>.csv).

    Returns:
        str: Main file name (without extension).

    Raises:
        ValueError: If metadata filename does not match expected pattern.
    """
    logger.info(f"Extracting main filename from metadata filename: {metadata_filename}")
    pattern = r'^SC360metadata_(.+)\.csv$'
    match = re.match(pattern, metadata_filename)
    if not match:
        logger.error(f"Invalid metadata filename format: {metadata_filename}")
        raise ValueError(f"Invalid metadata filename format: {metadata_filename}")
    main_filename = match.group(1)
    logger.info(f"Extracted main filename: {main_filename}")
    return main_filename

def extract_base_filename(main_filename):
    """
    Extract base file name from main file name in format: <base>_ddmmyy_hhmmss

    Args:
        main_filename (str): Main file name (e.g., EGI_2_0_AMS_INC_20250919090006).

    Returns:
        str: Extracted base file name.

    Raises:
        ValueError: If main filename does not match expected pattern.
    """
    logger.info(f"Extracting base filename from main filename: {main_filename}")
    pattern = r'^([A-Za-z0-9_\-]+)_(\d{14})$'
    match = re.match(pattern, main_filename)
    if not match:
        logger.error(f"Invalid main filename format: {main_filename}")
        raise ValueError(f"Invalid main filename format: {main_filename}")
    base = match.group(1)
    logger.info(f"Extracted base filename: {base}")
    return base

def get_expected_extension(conn, base_filename):
    """
    Queries audit_config table for expected file extension for the base filename.

    Args:
        conn (psycopg2.extensions.connection): Database connection object.
        base_filename (str): Extracted base file name.

    Returns:
        str: Expected file extension (e.g., 'csv', 'xlsx').

    Raises:
        Exception: If query fails or no extension is found.
    """
    logger.info(f"Querying audit_config for expected extension for base filename: {base_filename}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_format FROM audit.sc360_nrt_file_config WHERE file_key = %s LIMIT 1", (base_filename,))
            result = cur.fetchone()
            if result:
                expected_ext = result[0]
                logger.info(f"Found expected extension '{expected_ext}' for base filename '{base_filename}'")
                return expected_ext
            else:
                logger.error(f"No expected extension found in audit_config for base filename '{base_filename}'")
                raise ValueError(f"No expected extension found in audit_config for base filename '{base_filename}'")
    except Exception as e:
        logger.error(f"Error querying audit_config table: {e}")
        raise

def check_main_file_exists(s3_client, bucket, main_filename, expected_extension):
    """
    Checks if the main file exists in S3 with the specified extension.

    Args:
        s3_client: Boto3 S3 client.
        bucket (str): S3 bucket name.
        main_filename (str): Main file name (without extension).
        expected_extension (str): Expected file extension from config.

    Returns:
        str: Key of the main file in S3 if exists.

    Raises:
        FileNotFoundError: If main file is not found in S3.
    """
    key = f"LandingZone/{main_filename}.{expected_extension}"
    logger.info(f"Checking S3 for main file: s3://{bucket}/{key}")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"Main file exists: {key}")
        return key
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error(f"Main file not found in S3: {key}")
            raise FileNotFoundError(f"Main file not found in S3: {key}")
        else:
            logger.error(f"Error checking main file in S3: {e}")
            raise

def trigger_glue_job(bucket, main_key, metadata_key):
    """
    Triggers the Glue job with S3 bucket, main file key, and metadata file key as arguments.

    Args:
        bucket (str): S3 bucket name.
        main_key (str): S3 object key for main file.
        metadata_key (str): S3 object key for metadata file.

    Returns:
        str: AWS Glue job run ID.

    Raises:
        Exception: If Glue job fails to start.
    """
    glue = boto3.client('glue')
    job_name = os.environ['GLUE_JOB_NAME']
    logger.info(f"Triggering Glue job '{job_name}' for main file: s3://{bucket}/{main_key} and metadata file: s3://{bucket}/{metadata_key}")
    try:
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--S3_BUCKET': bucket,
                '--MAIN_KEY': main_key,
                '--METADATA_KEY': metadata_key
            }
        )
        job_run_id = response['JobRunId']
        logger.info(f"Glue job started successfully. JobRunId: {job_run_id}")
        return job_run_id
    except ClientError as e:
        logger.error(f"Glue job trigger failed: {e}")
        raise

def publish_sns_notification(topic_arn, subject, message):
    """
    Publishes a notification to the specified SNS topic.

    Args:
        topic_arn (str): ARN of the SNS topic.
        subject (str): Subject line for the notification.
        message (str): Message body.

    Returns:
        None

    Raises:
        Exception: If SNS publish fails.
    """
    sns = boto3.client('sns')
    try:
        logger.info(f"Publishing SNS notification with subject: '{subject}'")
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info("SNS notification published successfully.")
    except Exception as e:
        logger.error(f"Failed to publish SNS notification: {e}")

def lambda_handler(event, context):
    """
    AWS Lambda entry point. Processes metadata file event, checks main file, queries config,
    triggers Glue job, sends SNS notification for missing main file.

    Args:
        event (dict): AWS Lambda event payload (S3 or SQS event notification).
        context (object): AWS Lambda context object.

    Returns:
        dict: Result status and job_run_id if successful, or error message.
    """
    logger.info(f"Lambda triggered with event: {event}")

    secret_name = os.environ.get('RDS_SECRET_NAME')
    glue_job_name = os.environ.get('GLUE_JOB_NAME')
    sns_arn = os.environ.get('SNS_ARN')

    s3_client = boto3.client('s3')

    try:
        # Extract bucket and metadata file key from event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        metadata_key = record['s3']['object']['key']
        metadata_filename = metadata_key.split('/')[-1]
        logger.info(f"Processing metadata file: {metadata_filename} in bucket: {bucket}")

        # 1. Extract main filename from metadata filename
        try:
            main_filename = extract_main_filename_from_metadata(metadata_filename)
        except ValueError as ve:
            logger.error(f"Metadata filename validation failed: {ve}")
            if sns_arn:
                publish_sns_notification(
                    sns_arn,
                    subject="Metadata File Validation Failed",
                    message=f"Metadata file '{metadata_filename}' in bucket '{bucket}' failed validation: {ve}"
                )
            return {"status": "error", "message": str(ve)}

        # 2. Extract base filename from main filename (for config lookup)
        try:
            base_filename = extract_base_filename(main_filename)
        except ValueError as ve:
            logger.error(f"Main filename validation failed: {ve}")
            if sns_arn:
                publish_sns_notification(
                    sns_arn,
                    subject="Main File Validation Failed",
                    message=f"Main filename '{main_filename}' failed validation: {ve}"
                )
            return {"status": "error", "message": str(ve)}
        
        # 3. Connect to RDS and get expected extension for main file
        try:
            conn = get_db_connection(secret_name)
            expected_extension = get_expected_extension(conn, base_filename)
        except Exception as db_err:
            logger.error(f"RDS connection or config lookup failed: {db_err}")
            return {"status": "error", "message": "DB connection or config lookup error"}
        finally:
            if 'conn' in locals():
                conn.close()
                logger.info("Database connection closed.")
        
        # 4. Check if main file exists in S3
        try:
            main_key = check_main_file_exists(s3_client, bucket, main_filename, expected_extension)
        except FileNotFoundError as fnf_err:
            logger.error(f"Main file not found: {fnf_err}")
            if sns_arn:
                publish_sns_notification(
                    sns_arn,
                    subject="Main File Missing",
                    message=f"Main file '{main_filename}.{expected_extension}' not found in bucket '{bucket}' while processing metadata file '{metadata_filename}'."
                )
            return {"status": "error", "message": str(fnf_err)}
        
        # 5. Trigger Glue job for both main and metadata files
        try:
            job_run_id = trigger_glue_job(bucket, main_key, metadata_key)
            logger.info(f"Glue job started for main file: {main_key}, metadata file: {metadata_key}, JobRunId: {job_run_id}")
        except Exception as glue_err:
            logger.error(f"Failed to trigger Glue job: {glue_err}")
            return {"status": "error", "message": "Glue job trigger failed"}

        logger.info("Pipeline completed successfully.")
        return {"status": "success", "job_run_id": job_run_id}
    except Exception as e:
        logger.error(f"Unhandled error in pipeline: {e}")
        return {"status": "error", "message": str(e)}