import sys
import os
import logging
import json
import ast
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat_ws, lit, current_timestamp

# Import the dynamic Logging class
from logging_util import Logging
from file_validation import FileValidator
from data_validation import DataValidator

class GlueMainPipeline:
    """
    Main Glue job for S3→Redshift NRT Pipeline, using Redshift Data API for upsert/merge.
    """

    def __init__(self):
        self.args = self._parse_args()
        self.spark = SparkSession.builder.appName("MainGlueJob").getOrCreate()
        self.glueContext = GlueContext(SparkContext.getOrCreate())
        self.s3 = boto3.client('s3')
        self.redshift_data = boto3.client('redshift-data', region_name=os.environ.get("AWS_REGION", "us-east-1"))
        self.rds_data = boto3.client('rds-data', region_name=os.environ.get("AWS_REGION", "us-east-1"))
        self._load_config()
        self.redshift_secret_arn = self.args['REDSHIFT_SECRET_ARN']
        self.redshift_database = self.args['REDSHIFT_DATABASE']
        self.redshift_cluster_id = self.args['REDSHIFT_CLUSTER_ID']
        self.curated_table = self.args['CURATED_TABLE']
        self.published_table = self.args['PUBLISHED_TABLE']
        # Logging initialization (dynamic, reusable for all pipeline stages)
        self.logger = Logging(
            spark_context=self.spark,
            app_name="NRT_PIPELINE",
            log_file_name=f"{self.args['CURATED_TABLE']}_{self.args['MAIN_FILE_KEY']}.log"
        )

    def _parse_args(self):
        args = getResolvedOptions(
            sys.argv,
            [
                'MAIN_FILE_KEY', 'METADATA_FILE_KEY', 'CONFIG_S3_PATH',
                'CURATED_TABLE', 'PUBLISHED_TABLE', 'REDSHIFT_SECRET_ARN',
                'REDSHIFT_DATABASE', 'REDSHIFT_CLUSTER_ID', 'RDS_SECRET_ARN',
                'RDS_RESOURCE_ARN', 'RDS_DATABASE', 'REGION', 'ALLOWED_PREFIXES',
                'SNS_ARN', 'JOB_NAME', 'LOG_GROUP', 'JOB_RUN_ID', 'PIPELINE_ID', 'ENV'
            ]
        )
        self.sns_arn = args['SNS_ARN']
        self.job_name = args['JOB_NAME']
        self.log_group = args['LOG_GROUP']
        self.job_run_id = args['JOB_RUN_ID']
        self.pipeline_id = args['PIPELINE_ID']
        self.env = args['ENV']
        return args

    def _load_config(self):
        try:
            s3_path = self.args['CONFIG_S3_PATH']
            bucket, key = s3_path.replace('s3://', '').split('/', 1)
            resp = self.s3.get_object(Bucket=bucket, Key=key)
            self.config = json.loads(resp['Body'].read())
            self.logger.info(f"Loaded config from {self.args['CONFIG_S3_PATH']}")
        except Exception as e:
            self.logger.error(f"Failed to load config from S3: {e}")
            self._send_sns("ConfigLoad", f"Failed to load config: {e}", s3_path)
            raise

    def _get_config_for_file(self, file_key):
        import re
        for name, conf in self.config.items():
            pattern = conf.get("file_pattern")
            if re.match(pattern, os.path.basename(file_key)):
                self.logger.info(f"Config found for file_key {file_key}: {name}")
                return conf
        self.logger.error(f"No config found for file_key: {file_key}")
        self._send_sns("ConfigMatch", f"No config for {file_key}", file_key)
        raise ValueError(f"No config for {file_key}")

    def run(self):
        try:
            file_config = self._get_config_for_file(self.args['MAIN_FILE_KEY'])
            expected_extension = file_config['input_file_extension'].lstrip('.')

            # 1. File Validation
            fv = FileValidator(
                bucket=self.args['MAIN_FILE_KEY'].split('/')[0] if '/' in self.args['MAIN_FILE_KEY'] else '',
                region=self.args['REGION'],
                allowed_prefixes=self.args['ALLOWED_PREFIXES'].split(","),
                rds_secret_name=self.args['RDS_SECRET_ARN'],
                expected_extension=expected_extension
            )
            if not fv.validate_file(
                file_key=self.args['MAIN_FILE_KEY'],
                metadata_key=self.args['METADATA_FILE_KEY'],
                file_config=file_config
            ):
                msg = f"File validation failed for {self.args['MAIN_FILE_KEY']}."
                self.logger.error(msg)
                self._send_sns("FileValidation", msg, self.args['MAIN_FILE_KEY'])
                return

            self.logger.info("File validation passed.")
            self._send_sns("FileValidation", "File validation passed.", self.args['MAIN_FILE_KEY'])

            # 2. Data Reading
            s3_uri = f"s3://{self.args['MAIN_FILE_KEY']}"
            self.logger.info(f"Reading data file from {s3_uri} with Spark...")
            if expected_extension == "csv":
                df = self.spark.read.option("header", "true").csv(s3_uri)
            elif expected_extension == "xlsx":
                import pandas as pd
                import io
                s3 = boto3.resource('s3')
                bucket, key = self.args['MAIN_FILE_KEY'].replace("s3://", "").split("/", 1)
                obj = s3.Object(bucket, key)
                body = obj.get()['Body'].read()
                xl = pd.ExcelFile(io.BytesIO(body))
                df_pd = xl.parse(file_config.get("sheet_name", "Sheet1"))
                df = self.spark.createDataFrame(df_pd)
            else:
                msg = f"Unsupported file extension: {expected_extension}"
                self.logger.error(msg)
                self._send_sns("FileRead", msg, s3_uri)
                return

            # 3. Data Validation
            dv = DataValidator(file_config)
            if not dv.validate_data(df):
                msg = f"Data validation failed for {self.args['MAIN_FILE_KEY']}."
                self.logger.error(msg)
                self._send_sns("DataValidation", msg, self.args['MAIN_FILE_KEY'])
                return

            self.logger.info("Data validation passed.")
            self._send_sns("DataValidation", "Data validation passed.", self.args['MAIN_FILE_KEY'])

            # 4. Generate hash column
            key_cols = file_config.get("hash_columns") or file_config.get("primary_key_columns")
            if not key_cols:
                msg = "No hash columns or primary key columns defined in config."
                self.logger.error(msg)
                self._send_sns("HashKey", msg, self.args['MAIN_FILE_KEY'])
                return
            df = df.withColumn("hash_col", md5(concat_ws("||", *key_cols)))

            # 5. Versioning (SCD2 or as per config)
            if file_config.get("incremental_load", {}).get("enabled", False):
                df = self._apply_versioning(df)

            # 6. Write to Redshift curated table via JDBC
            self.logger.info(f"Writing to Redshift curated table {self.curated_table}")
            self._send_sns("CuratedLoad", "Writing to curated table", self.curated_table)
            self._write_to_redshift_jdbc(df, self.curated_table, mode="overwrite")
            self._send_sns("CuratedLoad", "Curated table write completed", self.curated_table)

            # 7. Upsert/Merge into published Redshift table via Data API SQL
            mode = "incremental" if file_config.get("incremental_load", {}).get("enabled", False) else "full"
            key_columns = file_config.get("primary_key_columns")
            self.logger.info(f"Merging data to published table {self.published_table} (mode={mode})")
            self._send_sns("PublishedMerge", f"Starting merge to published table (mode={mode})", self.published_table)
            self._upsert_to_published_redshift(
                curated_table=self.curated_table,
                published_table=self.published_table,
                mode=mode,
                key_columns=key_columns,
                hash_col="hash_col"
            )
            self._send_sns("PipelineSuccess", "File processed and published successfully.", self.published_table)
            self.logger.info("Glue Redshift pipeline completed successfully.")

        except Exception as e:
            tb = traceback.format_exc()
            self.logger.error(f"Glue main job failed: {e}\n{tb}")
            self._send_sns("PipelineFailure", f"Glue main job failed: {e}", "")

    def _apply_versioning(self, df):
        try:
            df = df.withColumn("is_active", lit(True)) \
                   .withColumn("effective_ts", current_timestamp()) \
                   .withColumn("end_ts", lit(None).cast("timestamp"))
            self.logger.info("Versioning columns added for SCD2.")
            return df
        except Exception as e:
            self.logger.error(f"Error applying versioning: {e}")
            raise

    def _write_to_redshift_jdbc(self, df, table, mode="append"):
        try:
            creds = self._get_redshift_creds()
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:redshift://{creds['host']}:{creds['port']}/{creds['database']}") \
                .option("dbtable", table) \
                .option("user", creds['username']) \
                .option("password", creds['password']) \
                .option("driver", "com.amazon.redshift.jdbc.Driver") \
                .mode(mode) \
                .save()
            self.logger.info(f"Data written to Redshift table {table}.")
        except Exception as e:
            self.logger.error(f"Error writing to Redshift table {table}: {e}")
            self._send_sns("CuratedLoad", f"Failed to write to curated table: {e}", table)
            raise

    def _get_redshift_creds(self):
        region_name = os.environ.get("AWS_REGION", "us-east-1")
        secrets_client = boto3.client('secretsmanager', region_name=region_name)
        resp = secrets_client.get_secret_value(SecretId=self.redshift_secret_arn)
        creds = ast.literal_eval(resp['SecretString'])
        return {
            "host": creds['redshift_host'],
            "port": creds['redshift_port'],
            "database": creds['redshift_database'],
            "username": creds['redshift_username'],
            "password": creds['redshift_password'],
        }

    def _upsert_to_published_redshift(self, curated_table, published_table, mode, key_columns, hash_col="hash_col"):
        try:
            if mode == "full":
                truncate_sql = f"TRUNCATE TABLE {published_table};"
                insert_sql = f"INSERT INTO {published_table} SELECT * FROM {curated_table};"
                self._run_redshift_sql(truncate_sql)
                self._run_redshift_sql(insert_sql)
                self.logger.info("Full load to published table complete.")
            elif mode == "incremental":
                merge_sql = f"""
                MERGE INTO {published_table} AS target
                USING {curated_table} AS src
                ON {" AND ".join([f"target.{col} = src.{col}" for col in key_columns])}
                WHEN MATCHED AND target.{hash_col} <> src.{hash_col}
                   THEN UPDATE SET
                     is_active = FALSE,
                     end_ts = GETDATE()
                WHEN NOT MATCHED
                   THEN INSERT *
                ;
                """
                self._run_redshift_sql(merge_sql)
                self.logger.info("Incremental SCD2 MERGE complete.")
            else:
                msg = f"Unknown upsert mode: {mode}"
                self.logger.error(msg)
                self._send_sns("PublishedMerge", msg, published_table)
                raise ValueError(msg)
        except Exception as e:
            self.logger.error(f"Error in post-load upsert: {e}")
            self._send_sns("PublishedMerge", f"Error in post-load upsert: {e}", published_table)
            raise

    def _run_redshift_sql(self, sql):
        response = self.redshift_data.execute_statement(
            ClusterIdentifier=self.redshift_cluster_id,
            Database=self.redshift_database,
            SecretArn=self.redshift_secret_arn,
            Sql=sql
        )
        self.logger.info(f"Redshift SQL executed. StatementId: {response['Id']}")
        return response

    def _send_sns(self, process_stage, message, s3_path):
        """
        Helper to send dynamic SNS for any pipeline stage.
        """
        self.logger.send_sns_message(
            pipelineid=self.pipeline_id,
            app_trigram="SC360",
            env=self.env,
            batch_region=self.args['REGION'],
            file_name=self.args['MAIN_FILE_KEY'],
            process_stage=process_stage,
            message=message,
            s3_path=s3_path,
            job_name=self.job_name,
            log_group=self.log_group,
            job_run_id=self.job_run_id,
            sns_arn=self.sns_arn
        )

if __name__ == "__main__":
    job = GlueMainPipeline()
    job.run()