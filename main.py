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
    concat_ws, sha2, col, lit, current_timestamp
)
from pyspark.sql.types import TimestampType
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from logging_util import Logging
from file_validation import FileValidator
from data_validation import DataValidator
from sql_generator import SCD2SQLGenerator

def fiscal_quarter_from_date(date_val):
    """
    Derives fiscal quarter string from a date value.
    """
    if not date_val:
        return None
    if isinstance(date_val, str):
        if len(date_val) < 7:
            return None
        y, m = date_val[:4], int(date_val[5:7])
    elif hasattr(date_val, "year") and hasattr(date_val, "month"):
        y, m = str(date_val.year), date_val.month
    else:
        return None
    q = ((int(m) - 1) // 3) + 1
    return f"{y}-Q{q}"

def apply_column_mapping_with_withColumn(df, mapping):
    """
    Applies column mapping and transformation to a Spark DataFrame.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    fiscal_quarter_udf = udf(fiscal_quarter_from_date, StringType())
    mapped_df = df
    for tgt, spec in mapping.items():
        if "source" in spec:
            mapped_df = mapped_df.withColumn(tgt, col(spec["source"]))
        elif "lit" in spec:
            mapped_df = mapped_df.withColumn(tgt, lit(spec["lit"]))
        elif "concat_ws" in spec:
            sep = spec["concat_ws"].get("sep", "")
            cols = spec["concat_ws"]["columns"]
            mapped_df = mapped_df.withColumn(tgt, concat_ws(sep, *[col(c) for c in cols]))
        elif "function" in spec and spec["function"] == "fiscal_quarter_from_date":
            mapped_df = mapped_df.withColumn(tgt, fiscal_quarter_udf(col(spec["args"][0])))
    return mapped_df

class NRTGlueJob:
    """
    Main class for NRT Glue job processing.
    """
    def __init__(self):
        self.args = self._parse_args()
        self.spark = self._get_spark_session()
        self.logger = self._get_logger()
        self.full_config = self._load_full_config()
        self.config = self._load_config_from_s3(self.args['CONFIG_S3_PATH'])
        self.s3_client = boto3.client('s3')
        self.redshift_client = boto3.client('redshift-data', region_name='us-east-1')

    @staticmethod
    def _parse_args():
        """
        Parses AWS Glue job arguments.
        """
        return getResolvedOptions(
            sys.argv,
            [
                'S3_BUCKET', 'MAIN_KEY', 'METADATA_KEY', 'CONFIG_S3_PATH',
                'CURATED_TABLE', 'PUBLISHED_TABLE', 'FULL_CONFIG',
                'database', 'env', 'resourcearn',
                'schema', 'secretarn', 'sns_arn', 'KMS_ID', 'redshift_secret_name', 'rds_secret_name'
            ]
        )

    @staticmethod
    def _get_spark_session():
        """
        Initializes and returns a Spark session.
        """
        spark = (
            SparkSession.builder
            .appName("DQ")
            .config('spark.sql.codegen.wholeStage', 'false')
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", 20)
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        return spark

    def _get_logger(self):
        """
        Initializes and returns a logger.
        """
        return Logging(
            spark_context=self.spark,
            app_name="NRT_PIPELINE",
            log_file_name="NRTGluejob_dataload.log"
        )

    def _load_full_config(self):
        """
        Loads the full config JSON from the Glue argument.
        """
        return json.loads(self.args['FULL_CONFIG'])

    @staticmethod
    def _load_config_from_s3(config_s3_path):
        """
        Loads the config JSON file from S3.
        """
        s3 = boto3.client('s3')
        bucket, key = config_s3_path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read())

    def _send_notification(self, error_message):
        """
        Sends SNS notification in case of failure.
        """
        try:
            sns = boto3.client('sns')
            sns.publish(
                TargetArn=self.args['sns_arn'],
                Message=error_message,
                Subject='NRT Data Validation Failure'
            )
            self.logger.info("SNS notification sent.")
        except Exception as exc:
            self.logger.error(f"Failed to send SNS notification: {exc}")
            self.logger.error(traceback.format_exc())

    def _get_entity_config(self):
        """
        Determines the entity config key based on the filename.
        """
        entity_name = os.path.splitext(os.path.basename(self.args['MAIN_KEY']))[0].split('_')[0]
        entity_config_key = None
        for k in self.config:
            if self.args['MAIN_KEY'].split('/')[-1].startswith(k):
                entity_config_key = k
                break
        if not entity_config_key:
            entity_config_key = list(self.config.keys())[0]  # fallback to first key
        return self.config[entity_config_key]

    def _list_s3_keys(self, prefix):
        """
        Lists S3 objects for a given prefix.
        """
        all_objects = self.s3_client.list_objects_v2(Bucket=self.args['S3_BUCKET'], Prefix=prefix)
        contents = all_objects.get('Contents', [])
        all_data_keys = [
            obj['Key'] for obj in contents
            if not obj['Key'].endswith('/') and not os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]
        all_metadata_keys = [
            obj['Key'] for obj in contents
            if os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]
        return all_data_keys, all_metadata_keys

    def _load_pandas_df(self, decrypted_bytes, expected_extension, entity_config):
        """
        Loads a pandas DataFrame from raw bytes based on extension.
        """
        if expected_extension.lower() == "csv":
            df = pd.read_csv(StringIO(decrypted_bytes.decode("utf-8")))
        elif expected_extension.lower() in ("xlsx", "xls"):
            excel_engine = entity_config.get("excel_engine", "openpyxl")
            df = pd.read_excel(BytesIO(decrypted_bytes), dtype=str, engine=excel_engine)
        else:
            raise Exception(f"Unsupported extension: {expected_extension}")
        return df

    def _rename_and_clean_pandas_df(self, pandas_df, column_type_map):
        """
        Renames columns based on config and replaces null values.
        """
        pandas_df.columns = list(column_type_map.keys())
        pandas_df = pandas_df.replace({np.nan: None, pd.NaT: None})
        return pandas_df

    def _execute_redshift_sql(self, sql, cluster_id=None, database=None, db_user=None):
        """
        Executes a SQL statement on Redshift using Redshift Data API.
        """
        if cluster_id is None:
            cluster_id = 'sc360-dev-redshiftcluster'
        if database is None:
            database = 'devdb'
        if db_user is None:
            db_user = 'arubauser'

        response = self.redshift_client.execute_statement(
            ClusterIdentifier=cluster_id,
            Database=database,
            DbUser=db_user,
            Sql=sql
        )
        print(f"Executed SQL on Redshift:\n{sql}")
        print(f"Redshift response: {response}")
        return response

    def _write_to_redshift(self, spark_df, table_options):
        """
        Writes a Spark DataFrame to Redshift using AWS Glue DynamicFrame.
        """
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        dyf = DynamicFrame.fromDF(spark_df, glue_context, "dyf_final")
        glue_context.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options=table_options
        )
        print("Data written to Redshift table successfully.")

    def run(self):
        """
        Main entry point to execute the Glue job.
        """
        # --- Log job parameters and configuration ---
        self.logger.info(f"Processing file: {self.args['MAIN_KEY']}")
        self.logger.info(f"Using curated table: {self.args['CURATED_TABLE']}")
        self.logger.info(f"Config S3 path: {self.args['CONFIG_S3_PATH']}")
        self.logger.info(f"Metadata file: {self.args['METADATA_KEY']}")
        self.logger.info(f"Environment: {self.args['env']}")
        self.logger.info("--- FULL CONFIG FROM DATABASE ROW ---")
        self.logger.info(json.dumps(self.full_config, indent=2))
        self.logger.info("--- CONFIG FILE FROM S3 (FOR FILE TYPE) ---")
        self.logger.info(json.dumps(self.config, indent=2))

        entity_config = self._get_entity_config()
        print("Entity config loaded and parsed.")

        allowed_prefixes = entity_config.get("allowed_prefixes", [""])
        expected_extension = entity_config.get("input_file_extension", "csv")
        rds_secret_name = self.args['secretarn']
        kms_id = self.args['KMS_ID']

        landing_prefix = os.path.dirname(self.args['MAIN_KEY'])
        all_data_keys, all_metadata_keys = self._list_s3_keys(landing_prefix)
        print(f"Listed S3 keys. Data files: {len(all_data_keys)}, Metadata files: {len(all_metadata_keys)}")

        # --- File Validation ---
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
                print("File validation failed. Notification sent. Exiting job.")
                return

            print("File validation PASSED.")

            # --- Load Data as DataFrame ---
            pandas_df = self._load_pandas_df(decrypted_bytes, expected_extension, entity_config)
            print("Loaded file into pandas DataFrame.")

            # --- Rename and Clean DataFrame ---
            column_type_map = entity_config.get("column_type_map", {})
            pandas_df = self._rename_and_clean_pandas_df(pandas_df, column_type_map)
            print("Renamed and cleaned pandas DataFrame.")

            # --- Convert to Spark DataFrame ---
            spark_df = self.spark.createDataFrame(pandas_df)
            print("Converted pandas DataFrame to Spark DataFrame.")

            # --- Data Validation ---
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
                self._send_notification(f"File rejected during DQ: {audit_info}")
                print("Data validation failed. Notification sent. Exiting job.")
                return

            print("Data validation PASSED, continuing downstream processing.")

            # --- Column Mapping ---
            column_mapping = entity_config.get("column_mapping", {})
            mapped_df = apply_column_mapping_with_withColumn(clean_df, column_mapping)
            print("Applied column mapping and transformations.")

            # --- Hash Generation ---
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
                    .withColumn("create_dt", lit(current_timestamp()).cast(TimestampType()))
                    .withColumn("source_nm", lit("sc360-dev-nrt-dataload-processor"))
                )
                print(f"Hash column '{hash_column_name}' generated using columns {hash_columns}.")
            else:
                final_df = mapped_df
                self.logger.warning("No hash_columns defined in config; hash_code not generated.")

            # --- Truncate Redshift table for fresh load ---
            cluster_id = 'sc360-dev-redshiftcluster'
            database = 'devdb'
            db_user = 'arubauser'
            truncate_sql = 'TRUNCATE TABLE curated.CUR_REVENUE_EGI_NRT;'
            self._execute_redshift_sql(truncate_sql)
            print(f"Redshift table curated.CUR_REVENUE_EGI_NRT truncated successfully.")

            # --- Write to Redshift curated table ---
            redshift_options = {
                "url": "jdbc:redshift://sc360-dev-redshiftcluster.cjjh5rkigghp.us-east-1.redshift.amazonaws.com:5439/devdb",
                "user": "arubauser",
                "password": "2020Sept01",
                "dbtable": "curated.CUR_REVENUE_EGI_NRT",
                "redshiftTmpDir": "s3://sc360-dev-ww-nrt-bucket/staging/",
                "mode": "overwrite"
            }
            self._write_to_redshift(final_df, redshift_options)

            # --- SCD2 SQL Generation from Config ---
            hash_columns = entity_config.get("hash_columns", [])
            primary_keys = entity_config.get("primary_key_columns", [])
            target_columns = entity_config.get("hash_columns", [])  # that's all you need!
            target_table = entity_config.get("published_table", "published.r_revenue_egi_nrt")
            staging_table = entity_config.get("curated_table", "curated.CUR_REVENUE_EGI_NRT")
            hash_key = entity_config.get("hash_column_name", "record_hash")

            sql_gen = SCD2SQLGenerator(
                target_table=target_table,
                staging_table=staging_table,
                primary_key_columns=primary_keys,
                hash_columns=hash_columns,
                surrogate_key="r_revenue_egi_nrt_id"
            )

            update_sql = sql_gen.generate_update_sql()
            insert_sql = sql_gen.generate_insert_sql()
            print("--- SCD2 UPDATE SQL ---\n", update_sql)
            print("--- SCD2 INSERT SQL ---\n", insert_sql)

            # --- Execute SCD2 UPDATE ---
            self._execute_redshift_sql(update_sql)
            print("SCD2 UPDATE executed successfully.")

            # --- Execute SCD2 INSERT ---
            self._execute_redshift_sql(insert_sql)
            print("SCD2 INSERT executed successfully.")

            print("Glue job completed successfully.")

        except Exception as exc:
            error_message = f"Exception during file or data validation: {str(exc)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            self._send_notification(error_message)
            print("Exception caught; failure handled and notification sent.")

def main():
    """
    Main function entry point.
    """
    job = NRTGlueJob()
    job.run()

if __name__ == "__main__":
    main()
