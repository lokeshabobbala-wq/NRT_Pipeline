import sys
import os
import json
import boto3
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from io import BytesIO

from logging_util import Logging
from file_validation import FileValidator
from data_validation import DataValidator  # <-- Import the new class

class NRTGlueJob:
    def __init__(self):
        self.args = self._parse_args()
        self.spark = SparkSession.builder.appName("DQ").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.logger = Logging(
            spark_context=self.spark,
            app_name="NRT_PIPELINE",
            log_file_name=f"NRTGluejob_dataload.log"
        )
        self.full_config = json.loads(self.args['FULL_CONFIG'])
        self.config = self._load_config_from_s3(self.args['CONFIG_S3_PATH'])

    @staticmethod
    def _parse_args():
        args = getResolvedOptions(
            sys.argv,
            [
                'S3_BUCKET', 'MAIN_KEY', 'METADATA_KEY', 'CONFIG_S3_PATH',
                'CURATED_TABLE', 'PUBLISHED_TABLE', 'FULL_CONFIG',
                'database', 'env', 'resourcearn',
                'schema', 'secretarn', 'sns_arn', 'KMS_ID'
            ]
        )
        return args

    @staticmethod
    def _load_config_from_s3(config_s3_path):
        s3 = boto3.client('s3')
        bucket, key = config_s3_path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read())

    def _send_notification(self, error_message):
        try:
            sns = boto3.client('sns')
            sns.publish(
                TargetArn=self.args['sns_arn'],
                Message=error_message,
                Subject='NRT Data Validation Failure'
            )
            self.logger.info("SNS notification sent.")
        except Exception as e:
            self.logger.error(f"Failed to send SNS notification: {e}")
            self.logger.error(traceback.format_exc())

    def run(self):
        self.logger.info(f"Processing file: {self.args['MAIN_KEY']}")
        self.logger.info(f"Using curated table: {self.args['CURATED_TABLE']}")
        self.logger.info(f"Config S3 path: {self.args['CONFIG_S3_PATH']}")
        self.logger.info(f"Metadata file: {self.args['METADATA_KEY']}")
        self.logger.info(f"Environment: {self.args['env']}")

        self.logger.info("--- FULL CONFIG FROM DATABASE ROW ---")
        self.logger.info(json.dumps(self.full_config, indent=2))
        self.logger.info("--- CONFIG FILE FROM S3 (FOR FILE TYPE) ---")
        self.logger.info(json.dumps(self.config, indent=2))

        # --- Load entity config ---
        entity_name = os.path.splitext(os.path.basename(self.args['MAIN_KEY']))[0].split('_')[0]
        # Use explicit key or fallback to known config section
        entity_config_key = None
        for k in self.config.keys():
            if self.args['MAIN_KEY'].split('/')[-1].startswith(k):
                entity_config_key = k
                break
        if not entity_config_key:
            entity_config_key = list(self.config.keys())[0]  # fallback to first key

        entity_config = self.config[entity_config_key]
        allowed_prefixes = entity_config.get("allowed_prefixes", [""])
        expected_extension = entity_config.get("input_file_extension", "csv")
        rds_secret_name = self.args['secretarn']
        kms_id = self.args['KMS_ID']

        s3 = boto3.client('s3')
        landing_prefix = os.path.dirname(self.args['MAIN_KEY'])
        all_objects = s3.list_objects_v2(Bucket=self.args['S3_BUCKET'], Prefix=landing_prefix)
        all_data_keys = [obj['Key'] for obj in all_objects.get('Contents', []) if not obj['Key'].endswith('/') and not os.path.basename(obj['Key']).startswith('SC360metadata_')]
        all_metadata_keys = [obj['Key'] for obj in all_objects.get('Contents', []) if os.path.basename(obj['Key']).startswith('SC360metadata_')]

        validator = FileValidator(
            bucket=self.args['S3_BUCKET'],
            region=os.environ.get("AWS_REGION", "us-east-1"),
            logger=self.logger,
            allowed_prefixes=allowed_prefixes,
            rds_secret_name=rds_secret_name,
            expected_extension=expected_extension,
            kms_id=kms_id
        )

        file_key = self.args['MAIN_KEY']
        metadata_key = self.args['METADATA_KEY']

        try:
            self.logger.info("Starting file validation...")
            valid, decrypted_bytes = validator.validate_file(
                file_key, metadata_key, entity_config,
                all_data_keys=all_data_keys, all_metadata_keys=all_metadata_keys
            )
            if valid:
                self.logger.info("File validation PASSED.")
                # Load file into pandas DataFrame
                import pandas as pd
                from io import BytesIO, StringIO

                if expected_extension.lower() == "csv":
                    pandas_df = pd.read_csv(StringIO(decrypted_bytes.decode("utf-8")), dtype=str)
                elif expected_extension.lower() in ("xlsx", "xls"):
                    excel_engine = entity_config.get("excel_engine", "openpyxl")
                    pandas_df = pd.read_excel(
                        BytesIO(decrypted_bytes),
                        dtype=str,
                        engine=excel_engine
                    )
                else:
                    raise Exception(f"Unsupported extension: {expected_extension}")

                # Rename columns by position
                pandas_df.columns = entity_config['required_columns']
                
                spark_df = self.spark.createDataFrame(pandas_df.astype(str).replace('nan', '').replace('NaT', ''))

                # --- DATA VALIDATION (DQ) ---
                self.logger.info("Starting data validation (DQ checks)...")
                dq_validator = DataValidator(
                    df=spark_df,
                    config=entity_config,
                    logger=self.logger,
                    file_name=os.path.basename(file_key)
                )
                clean_df = dq_validator.run_all_checks()
                audit_info = dq_validator.get_audit_info()

                if clean_df is None:
                    self.logger.error(f"File rejected during data validation: {audit_info}")
                    # --- AUDIT & REJECTION HANDLING ---
                    # You can do: dq_validator.write_rejected_records(...) or custom S3/DB logic here
                    # Optionally send notification:
                    self._send_notification(f"File rejected during DQ: {audit_info}")
                else:
                    self.logger.info("Data validation PASSED, continuing downstream processing.")
                    # Continue with clean_df
                    # For example: clean_df.write.mode("overwrite").saveAsTable(...)
                    # Or further business logic, audit log update, etc.

            else:
                self.logger.error("File validation FAILED.")
                error_message = f"File validation failed for {file_key}. See log for details."
                self._send_notification(error_message)
        except Exception as e:
            error_message = f"Exception during file or data validation: {str(e)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            self._send_notification(error_message)

if __name__ == "__main__":
    job = NRTGlueJob()
    job.run()
