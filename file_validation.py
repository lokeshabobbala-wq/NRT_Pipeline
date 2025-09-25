"""
file_validation.py

File validation package for S3 data pipeline, to be imported into Glue jobs.

Implements all validation steps as methods on FileValidator, following the
coding standards and docstring/logging/error-handling style of sc360-dev-nrt-dataload-trigger.py.
"""

import os
import logging
import boto3
import csv
import codecs
import psycopg2
import datetime as dt
import ast
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class FileValidator:
    """
    Class for validating files landed in S3 as per pipeline checklist.

    Usage:
        validator = FileValidator(bucket, region, ...)
        result = validator.validate_file(file_key)
    """

    def __init__(self, bucket, region, allowed_prefixes, rds_secret_name, expected_extension, max_sla_hours=2):
        """
        Initialize FileValidator.

        Args:
            bucket (str): S3 bucket name.
            region (str): Region name.
            allowed_prefixes (list): Expected file prefixes for name validation.
            rds_secret_name (str): Secret name for RDS credentials (for audit logging).
            expected_extension (str): Allowed file extension (e.g. 'csv').
            max_sla_hours (int): Time window for SLA check.
        """
        self.bucket = bucket
        self.region = region
        self.allowed_prefixes = allowed_prefixes
        self.expected_extension = expected_extension
        self.max_sla_hours = max_sla_hours
        self.s3 = boto3.client('s3')
        self.rds_secret_name = rds_secret_name
        self.conn = self.get_db_connection(rds_secret_name)

    def get_db_connection(self, secret_name):
        """
        Get psycopg2 connection using AWS Secrets Manager.
        """
        try:
            region_name = os.environ.get("AWS_REGION", "us-east-1")
            secrets_client = boto3.client('secretsmanager', region_name=region_name)
            logger.info(f"Fetching RDS credentials from Secrets Manager: {secret_name}")
            response = secrets_client.get_secret_value(SecretId=secret_name)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                creds = ast.literal_eval(response['SecretString'])
                conn_string = (
                    f"dbname='{creds['engine']}' port='{creds['port']}' user='{creds['username']}' "
                    f"password='{creds['password']}' host='{creds['host']}'"
                )
                logger.info("Establishing database connection to RDS...")
                connection = psycopg2.connect(conn_string)
                logger.info("Database connection successful.")
                return connection
            else:
                logger.error("Failed to retrieve credentials for RDS.")
                raise RuntimeError("Failed to retrieve credentials for RDS.")
        except Exception as e:
            logger.error(f"Error getting RDS DB connection: {e}")
            raise

    def list_s3_objects(self, prefix):
        """
        List all object keys under the given prefix.
        """
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            keys = []
            for page in page_iterator:
                keys.extend([obj['Key'] for obj in page.get('Contents', [])])
            logger.info(f"Found {len(keys)} objects under prefix {prefix}")
            return keys
        except Exception as e:
            logger.error(f"Error listing objects in S3: {e}")
            raise

    def check_file_presence(self, key):
        """
        Check if a file exists in S3.
        """
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            logger.info(f"File found: s3://{self.bucket}/{key}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.warning(f"File not found: s3://{self.bucket}/{key}")
                return False
            logger.error(f"Error in head_object: {e}")
            raise

    def check_file_naming_pattern(self, filename):
        """
        Check if filename starts with any allowed prefix.
        """
        is_valid = any(filename.startswith(prefix) for prefix in self.allowed_prefixes)
        logger.info(f"Naming pattern check for {filename}: {is_valid}")
        return is_valid

    def find_metadata_file(self, data_filename, metadata_files):
        """
        Return the matching metadata file for a given data file, or None.
        """
        base = data_filename.split('.')[0]
        for m in metadata_files:
            if m.startswith(f"SC360metadata_{base}"):
                logger.info(f"Matching metadata file for {data_filename}: {m}")
                return m
        logger.warning(f"No metadata found for {data_filename}")
        return None

    def check_file_size(self, key):
        """
        Check file size > 0.
        """
        size = self.s3.head_object(Bucket=self.bucket, Key=key)['ContentLength']
        logger.info(f"File {key} size: {size}")
        return size > 0

    def check_file_size_matches_metadata(self, data_key, metadata_key):
        """
        Check that file size matches value in metadata file.
        """
        try:
            file_size = self.s3.head_object(Bucket=self.bucket, Key=data_key)['ContentLength']
            m_obj = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
            metadataInfo = list(csv.reader(codecs.getreader("utf-8")(m_obj["Body"])))
            if not metadataInfo:
                logger.warning("Metadata file is empty.")
                return False
            size_from_meta = int(metadataInfo[0][0].split('|')[1])
            logger.info(f"File size from meta: {size_from_meta}, actual: {file_size}")
            return file_size == size_from_meta
        except Exception as e:
            logger.error(f"Error checking file size against metadata: {e}")
            return False

    def check_file_extension(self, filename):
        """
        Check the file extension.
        """
        ext = filename.split('.')[-1].lower()
        is_valid = ext == self.expected_extension.lower()
        logger.info(f"Extension check: {ext} vs allowed {self.expected_extension.lower()} = {is_valid}")
        return is_valid

    def check_kms_encryption(self, key):
        """
        Check if the file is encrypted with KMS.
        """
        resp = self.s3.head_object(Bucket=self.bucket, Key=key)
        enc = resp.get('ServerSideEncryption')
        logger.info(f"KMS encryption: {enc}")
        return enc == 'aws:kms'

    def check_file_integrity(self, key):
        """
        Check if file can be opened (CSV header read).
        """
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            next(csv.reader(codecs.getreader("utf-8")(obj["Body"])))
            logger.info(f"File {key} opened successfully (integrity pass).")
            return True
        except Exception as e:
            logger.warning(f"Failed to open file {key}: {e}")
            return False

    def check_timeliness(self, key):
        """
        Check if file is within allowed SLA window (default 2 hours).
        """
        import datetime
        last_modified = self.s3.head_object(Bucket=self.bucket, Key=key)['LastModified']
        now = datetime.datetime.utcnow().replace(tzinfo=last_modified.tzinfo)
        delta = (now - last_modified).total_seconds() / 3600
        is_valid = delta <= self.max_sla_hours
        logger.info(f"Timeliness check for {key}: {delta:.2f} hours old (valid={is_valid})")
        return is_valid

    def check_duplicates(self, file_list):
        """
        Check for duplicate data files by base name (excluding timestamp).
        """
        seen = set()
        for f in file_list:
            base = "_".join(f.split('/')[-1].split('_')[:-1])
            if base in seen:
                logger.warning("Duplicate file found: " + f)
                return True
            seen.add(base)
        logger.info("No duplicates found.")
        return False

    def audit_log(self, **kwargs):
        """
        Insert an audit log record. (Params should match DB columns.)
        """
        insert_sql = """
        INSERT INTO audit.sc360_audit_log(
            batchid, batchrundate, regionname, datapipelinename, pipelinerunid, processname,
            filename, sourcename, destinationname, sourceplatform, targetplatform, activity,
            environment, interval, scriptpath, executionstatus, errormessage, datareadsize,
            datawrittensize, logtimestamp)
        VALUES(%(BatchId)s, %(BatchRunDate)s, %(pRegionName)s, %(pDataPipelineName)s, %(pDataPipelineId)s,
               %(ProcessName)s, %(fileName)s, %(sourceName)s, %(destinationName)s, %(SourcePlatform)s, %(TargetPlatform)s,
               %(Activity)s, %(Environment)s, %(pScheduleType)s, %(emrMasterScriptPath)s, %(executionStatus)s, %(errorMessage)s,
               %(dataReadSize)s, %(dataWrittenSize)s, %(logTimestamp)s )
        """
        cursor = self.conn.cursor()
        cursor.execute(insert_sql, kwargs)
        self.conn.commit()
        logger.info(f"Audit log committed for {kwargs.get('fileName')}.")

    def move_to_folder(self, src_folder, dst_folder, file, meta_file=None):
        """
        Move a file (+ optional metadata file) to another folder (e.g. reject/quarantine/await).
        """
        def move_one(fname):
            if fname:
                src_key = f"{src_folder}{fname}"
                dst_key = f"{dst_folder}{fname}"
                self.s3.copy_object(Bucket=self.bucket, Key=dst_key, CopySource={'Bucket': self.bucket, 'Key': src_key})
                self.s3.delete_object(Bucket=self.bucket, Key=src_key)
                logger.info(f"Moved {src_key} to {dst_key}")
        move_one(file)
        if meta_file:
            move_one(meta_file)

    # Example main method to validate all files under a prefix
    def validate_all_files(self, landing_folder, reject_folder, await_folder, batch_id, batch_run_date, schedule_type, emr_script_path):
        """
        Run all file validation steps as per checklist on all data files in landing_folder.
        """
        try:
            all_keys = self.list_s3_objects(landing_folder)
            data_files = [k.split('/')[-1] for k in all_keys if not k.endswith('/') and not k.split('/')[-1].startswith('SC360metadata_')]
            metadata_files = [k.split('/')[-1] for k in all_keys if k.split('/')[-1].startswith('SC360metadata_')]

            for data_file in data_files:
                logger.info(f"Validating {data_file}")
                # 2. Naming
                if not self.check_file_naming_pattern(data_file):
                    self.move_to_folder(landing_folder, reject_folder, data_file)
                    continue
                # 3. Metadata
                meta_file = self.find_metadata_file(data_file, metadata_files)
                if not meta_file:
                    self.move_to_folder(landing_folder, await_folder, data_file)
                    continue
                # 4. Size
                if not self.check_file_size(f"{landing_folder}{data_file}"):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 5. Size match
                if not self.check_file_size_matches_metadata(f"{landing_folder}{data_file}", f"{landing_folder}{meta_file}"):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 6. Extension
                if not self.check_file_extension(data_file):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 7. KMS
                if not self.check_kms_encryption(f"{landing_folder}{data_file}"):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 8. Integrity
                if not self.check_file_integrity(f"{landing_folder}{data_file}"):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 9. Timeliness
                if not self.check_timeliness(f"{landing_folder}{data_file}"):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 10. Duplicates
                if self.check_duplicates(data_files):
                    self.move_to_folder(landing_folder, reject_folder, data_file, meta_file)
                    continue
                # 11. Audit log
                self.audit_log(
                    BatchId=batch_id,
                    BatchRunDate=batch_run_date,
                    pRegionName=self.region,
                    pDataPipelineName="NRT_Pipeline",
                    pDataPipelineId="24532",
                    ProcessName="FileValidation",
                    fileName=data_file,
                    sourceName=data_file,
                    destinationName=data_file,
                    SourcePlatform="S3",
                    TargetPlatform="S3",
                    Activity="Validation",
                    Environment=os.environ.get('env', 'dev'),
                    pScheduleType=schedule_type,
                    emrMasterScriptPath=emr_script_path,
                    executionStatus="ReadyToExecute",
                    errorMessage="",
                    dataReadSize=self.s3.head_object(Bucket=self.bucket, Key=f"{landing_folder}{data_file}")['ContentLength'],
                    dataWrittenSize=0,
                    logTimestamp=dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
                logger.info(f"{data_file} validated and logged as ReadyToExecute.")
        except Exception as e:
            logger.error(f"Unhandled error in file validation: {e}")
            raise
