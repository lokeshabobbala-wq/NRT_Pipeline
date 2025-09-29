import sys
import os
import json
import boto3
import traceback
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat_ws, sha2, col, lit, current_timestamp, udf, current_date
)
from pyspark.sql.types import (
    StringType, DateType, TimestampType, DecimalType
)
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from logging_util import Logging
from file_validation import FileValidator
from data_validation import DataValidator

def apply_column_mapping_with_withColumn(df, mapping):
    def fiscal_quarter_from_date(date_str):
        if not date_str or len(date_str) < 7:
            return None
        y, m = date_str[:4], int(date_str[5:7])
        q = ((m - 1) // 3) + 1
        return f"{y}-Q{q}"

    fiscal_quarter_udf = udf(fiscal_quarter_from_date, StringType())

    for tgt, spec in mapping.items():
        if "source" in spec:
            df = df.withColumn(tgt, col(spec["source"]))
        elif "lit" in spec:
            df = df.withColumn(tgt, lit(spec["lit"]))
        elif "concat_ws" in spec:
            sep = spec["concat_ws"].get("sep", "")
            cols = spec["concat_ws"]["columns"]
            df = df.withColumn(tgt, concat_ws(sep, *[col(c) for c in cols]))
        elif "function" in spec and spec["function"] == "fiscal_quarter_from_date":
            df = df.withColumn(tgt, fiscal_quarter_udf(col(spec["args"][0])))
    return df

class NRTGlueJob:
    def __init__(self):
        self.args = self._parse_args()
        self.spark = (
            SparkSession.builder
            .appName("DQ")
            .config('spark.sql.codegen.wholeStage', 'false')
            .getOrCreate()
        )
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.logger = Logging(
            spark_context=self.spark,
            app_name="NRT_PIPELINE",
            log_file_name="NRTGluejob_dataload.log"
        )
        self.full_config = json.loads(self.args['FULL_CONFIG'])
        self.config = self._load_config_from_s3(self.args['CONFIG_S3_PATH'])

    @staticmethod
    def _parse_args():
        return getResolvedOptions(
            sys.argv,
            [
                'S3_BUCKET', 'MAIN_KEY', 'METADATA_KEY', 'CONFIG_S3_PATH',
                'CURATED_TABLE', 'PUBLISHED_TABLE', 'FULL_CONFIG',
                'database', 'env', 'resourcearn',
                'schema', 'secretarn', 'sns_arn', 'KMS_ID'
            ]
        )

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

        # Load entity config
        entity_name = os.path.splitext(os.path.basename(self.args['MAIN_KEY']))[0].split('_')[0]
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
        all_data_keys = [
            obj['Key']
            for obj in all_objects.get('Contents', [])
            if not obj['Key'].endswith('/') and not os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]
        all_metadata_keys = [
            obj['Key']
            for obj in all_objects.get('Contents', [])
            if os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]

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
            if not valid:
                self.logger.error("File validation FAILED.")
                error_message = f"File validation failed for {file_key}. See log for details."
                self._send_notification(error_message)
                return

            self.logger.info("File validation PASSED.")

            # Load file into pandas DataFrame
            if expected_extension.lower() == "csv":
                pandas_df = pd.read_csv(StringIO(decrypted_bytes.decode("utf-8")))
            elif expected_extension.lower() in ("xlsx", "xls"):
                excel_engine = entity_config.get("excel_engine", "openpyxl")
                pandas_df = pd.read_excel(BytesIO(decrypted_bytes), engine=excel_engine)
            else:
                raise Exception(f"Unsupported extension: {expected_extension}")

            # Rename columns by position
            column_type_map = entity_config.get("column_type_map", {})
            pandas_df.columns = list(column_type_map.keys())

            # Replace np.nan and NaT with None (Spark will interpret as null)
            pandas_df = pandas_df.replace({np.nan: None, pd.NaT: None})
            print(pandas_df.head())
            # Create Spark DataFrame (types will be inferred)
            spark_df = self.spark.createDataFrame(pandas_df)

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
            column_type_map = entity_config['column_type_map']
            clean_df.show(10, truncate=False)
            if clean_df is None:
                self.logger.error(f"File rejected during data validation: {audit_info}")
                self._send_notification(f"File rejected during DQ: {audit_info}")
                return

            self.logger.info("Data validation PASSED, continuing downstream processing.")

            # --- Column Mapping ---
            column_mapping = entity_config.get("column_mapping", {})
            mapped_df = apply_column_mapping_with_withColumn(clean_df, column_mapping)
            #mapped_df.show(10, truncate=False)
            # ---- HASH GENERATION ----
            hash_columns = entity_config.get("hash_columns", [])
            hash_column_name = entity_config.get("hash_column_name", "record_hash")
            source_file_name = os.path.basename(file_key)
            if hash_columns:
                final_df = (
                    mapped_df
                    .withColumn(hash_column_name, sha2(concat_ws("|", *[col(c) for c in hash_columns]), 256))
                    .withColumn("source_file_name", lit(source_file_name))
                    .withColumn("batch_run_dt", lit(current_timestamp()).cast(TimestampType()))
                    .withColumn("created_by", lit("glue"))
                    .withColumn("create_dt", lit(current_date()).cast(DateType()))
                    .withColumn("source_nm", lit("sc360-dev-nrt-dataload-processor"))
                )
                self.logger.info(f"Hash column '{hash_column_name}' generated using columns {hash_columns}.")
            else:
                final_df = mapped_df
                self.logger.warning("No hash_columns defined in config; hash_code not generated.")
            #final_df.show(10, truncate=False)
            # Create the Redshift Data API client
            client = boto3.client('redshift-data', region_name='us-east-1')
            
            # Example usage
            response = client.execute_statement(
                ClusterIdentifier='sc360-dev-redshiftcluster',      # Your cluster identifier
                Database='devdb',                                   # Your Redshift database name
                DbUser='arubauser',                                 # Your Redshift database user
                Sql='TRUNCATE TABLE curated.CUR_REVENUE_EGI_NRT;'   # Your SQL statement
            )
            
            # Convert Spark DataFrame to AWS Glue DynamicFrame
            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            dyf = DynamicFrame.fromDF(final_df, glueContext, "dyf_final")
            
            # Redshift connection options
            redshift_options = {
                "url": "jdbc:redshift://sc360-dev-redshiftcluster.cjjh5rkigghp.us-east-1.redshift.amazonaws.com:5439/devdb",
                "user": "arubauser",
                "password": "2020Sept01",
                "dbtable": "curated.CUR_REVENUE_EGI_NRT",
                "redshiftTmpDir": "s3://sc360-dev-ww-nrt-bucket/staging/",  # <-- Replace with your S3 path!
                "mode": "overwrite"
            }

            # Write to Redshift using AWS Glue
            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="redshift",
                connection_options=redshift_options
            )
            
        except Exception as e:
            error_message = f"Exception during file or data validation: {str(e)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            self._send_notification(error_message)

if __name__ == "__main__":
    job = NRTGlueJob()
    job.run()
