import logging
import boto3
import json

class Logging:
    """
    Custom Logging class for AWS Glue/Spark jobs.
    - Prints to Spark log4j and Python stdout
    - Supports dynamic SNS notifications for ALL pipeline stages
    """

    def __init__(self, spark_context, app_name, log_file_name):
        log4jLogger = spark_context._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(app_name)

        # Python logging for stdout
        logging_format = "%(asctime)s %(levelname)s %(name)s %(message)s"
        logging.basicConfig(level=logging.INFO, filename=log_file_name, format=logging_format)
        self.stdout_logger = logging.getLogger(app_name)
        self.stdout_logger.addHandler(logging.StreamHandler())

    def info(self, log_message):
        self.logger.info(log_message)
        self.stdout_logger.info(log_message)

    def error(self, log_message):
        self.logger.error(log_message)
        self.stdout_logger.error(log_message)

    def debug(self, log_message):
        self.logger.debug(log_message)
        self.stdout_logger.debug(log_message)

    def send_sns_message(self,
                         pipelineid,
                         app_trigram,
                         env,
                         batch_region,
                         file_name,
                         process_stage,  # e.g. "FileValidation", "DataValidation", "CuratedLoad", "MergeToPublished", "Success", "Failure"
                         message,       # dynamic message, not just "Reject Reason"
                         s3_path,
                         job_name,
                         log_group,
                         job_run_id,
                         sns_arn,
                         extra_payload=None):
        """
        Send dynamic SNS notification for any pipeline stage.
        Args:
            pipelineid: Pipeline run id
            app_trigram: App code/name
            env: Environment
            batch_region: Data region
            file_name: Current file being processed
            process_stage: The stage (FileValidation, DataValidation, etc)
            message: The message or error/reason for notification
            s3_path: S3 path for reference
            job_name: Glue job name
            log_group: CloudWatch log group name
            job_run_id: Glue job run id
            sns_arn: SNS topic arn
            extra_payload: Dict of additional keys to send (optional)
        """
        sns_message = {
            "PipelineId": pipelineid,
            "App_Trigram": app_trigram,
            "Env": env,
            "Batch_Region": batch_region,
            "File": file_name,
            "Process_Stage": process_stage,
            "Message": message,
            "S3_Path": s3_path,
            "Glue_Job_Name": job_name,
            "Log_Group": log_group,
            "Log_Stream_ID": job_run_id
        }
        if extra_payload and isinstance(extra_payload, dict):
            sns_message.update(extra_payload)

        subject = f"{process_stage} - {file_name} [{env}]"
        sns = boto3.client('sns')
        sns.publish(
            TargetArn=sns_arn,
            Message=json.dumps(sns_message),
            Subject=subject
        )