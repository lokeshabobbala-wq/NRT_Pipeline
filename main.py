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

class NRTGlueJob:
    def __init__(self):
        self.args = self._parse_args()
        self.spark = SparkSession.builder.appName("DQ").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
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

    # def _update_audit_log(self, status, error_message=""):
    #     # Audit functionality temporarily disabled for testing.
    #     pass

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
        # Only log the key arguments needed for processing
        self.logger.info(f"Processing file: {self.args['MAIN_KEY']}")
        self.logger.info(f"Using curated table: {self.args['CURATED_TABLE']}")
        self.logger.info(f"Config S3 path: {self.args['CONFIG_S3_PATH']}")
        self.logger.info(f"Metadata file: {self.args['METADATA_KEY']}")
        self.logger.info(f"Environment: {self.args['env']}")

        self.logger.info("--- FULL CONFIG FROM DATABASE ROW ---")
        self.logger.info(json.dumps(self.full_config, indent=2))
        self.logger.info("--- CONFIG FILE FROM S3 (FOR FILE TYPE) ---")
        self.logger.info(json.dumps(self.config, indent=2))
        # ...other key log lines...
        entity_config = self.config["EGI_2_0_INC"]
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
            valid = validator.validate_file(
                file_key, metadata_key, entity_config,
                all_data_keys=all_data_keys, all_metadata_keys=all_metadata_keys
            )
            if valid:
                self.logger.info("File validation PASSED.")
                decrypted_bytes = validator.check_kms_encryption_and_decrypt(file_key)
                # If small: load to DataFrame directly
                import pandas as pd
                from io import BytesIO
                pandas_df = pd.read_excel(BytesIO(decrypted_bytes),dtype=str, engine='openpyxl')  # or engine='xlrd' for .xls
                spark_df = self.spark.createDataFrame(pandas_df.astype(str).replace('nan', '').replace('NaT', ''))
                # self._update_audit_log(status="Succeeded")
            else:
                self.logger.error("File validation FAILED.")
                error_message = "File validation failed for {}. See log for details.".format(file_key)
                # self._update_audit_log(status="Failed", error_message=error_message)
                self._send_notification(error_message)
        except Exception as e:
            error_message = f"Exception during file validation: {str(e)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            # self._update_audit_log(status="Failed", error_message=error_message)
            self._send_notification(error_message)

if __name__ == "__main__":
    job = NRTGlueJob()
    job.run()
