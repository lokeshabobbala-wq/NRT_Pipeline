import boto3
import os
import csv
import codecs
import psycopg2
from datetime import date
import datetime as dt
import aws_encryption_sdk
from aws_encryption_sdk.identifiers import CommitmentPolicy
import base64
import argparse
from argparse import ArgumentParser
import traceback
import sys
# import logging
import time
import json
import ast
from urllib.parse import urlparse
from botocore.exceptions import ClientError

# Environment variable
Environment = 'dev'  # enter environment
pRegionName = ''  # enter regionName
Schedule = 'Daily'  # enter schedule
pDataPipelineId = '24532'
pScheduleType = pRegionName + Schedule  # make pscheduletype
Batch = pRegionName + 'Batch'
ProcessName = 'FileValidation'
SourcePlatform = 'S3'
TargetPlatform = 'S3'
Activity = 'EMRActivity'
bucketName = 'sc360-' + Environment + '-' + pRegionName.lower() + '-bucket'
emrMasterScriptPath = 's3://' + bucketName + '/Resources/Scripts/EC2MasterScript/EC2MasterPythonScript.py'
# BatchRunDate = '2021-02-05'
file_keys = ['Z1VMGMT', 'Y1_MULTI_SEL_RPT', 'SAP_Inv_32F2', 'Reservation_list', 'AMS_OM_CrossBorderReport','Master_Data']


class decryption_class:
    def __init__(self, key_ids, landingBucketName, landingKey, bucketName, processingKey):
        # Get the configs
        self.key_ids = key_ids
        self.source_bucket_name = landingBucketName
        self.source_key_name = landingKey
        self.destination_bucket_name = bucketName
        self.destination_key_name = processingKey

        # Define the boto3 s3 client
        self.s3_client = boto3.client('s3')

        # Get the master key provider from the key_ids
        try:
            kms_kwargs = dict(key_ids=[self.key_ids], region_names=['us-east-1'])
            self.master_key_provider = aws_encryption_sdk.StrictAwsKmsMasterKeyProvider(**kms_kwargs)
        except Exception as e:
            print("Exception caught while creating the master key provider from Key Alias. Check the key alias value "
                  "being passed.")
            print(traceback.format_exc())
            print(e)
            sys.exit(1)

    def decrypt_s3_file_method(self):
        try:
            print("Started operating on " + self.source_key_name + " file.")
            # Reading the encrypted file using boto3 s3 client
            input_s3_object = self.s3_client.get_object(Bucket=self.source_bucket_name,
                                                        Key=self.source_key_name)

            # Proceed only if the s3_client has read the file successfully
            if str(input_s3_object['ResponseMetadata']['HTTPStatusCode']) == '200':
                print("Decryption starts.")
                contents = input_s3_object['Body'].read()
                client = aws_encryption_sdk.EncryptionSDKClient(
                    commitment_policy=CommitmentPolicy.FORBID_ENCRYPT_ALLOW_DECRYPT
                )
                decoded_ciphertext = base64.b64decode(contents)
                cycled_plaintext, decrypted_header = client.decrypt(source=decoded_ciphertext,
                                                                    key_provider=self.master_key_provider)

                print("Decryption completed. Now putting the decrypted file back to destination path.")

                put_object_response = self.s3_client.put_object(
                    Body=cycled_plaintext,
                    Bucket=self.destination_bucket_name,
                    Key=self.destination_key_name,
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=self.key_ids
                )

                if str(put_object_response['ResponseMetadata']['HTTPStatusCode']) == '200':
                    print("The decrypted file " + self.destination_key_name + " is successfully placed back to AWS S3")
                else:
                    raise Exception("The decrypted file placement in AWS S3 got failed.")
                    print(traceback.format_exc())
                    sys.exit(1)
            else:
                raise Exception("The encrypted file reading from AWS S3 got failed.")
                print(traceback.format_exc())
                sys.exit(1)
        except Exception as e:
            print("Exception caught while decrypting the encrypted file.")
            print(traceback.format_exc())
            print(e)
            sys.exit(1)


# function to decrypt and archive the files
def decrypt_and_archive(landingBucketName, landingFolder, bucketName, processingFolder, archiveFolder,validDataAndMetaDataFileDict):
    #     Define the variable for decrypting

    # key_ids = 'SC360-AURBA-DEV-CMK'
    key_ids = os.environ['KMS_id']
    s3_client = boto3.client('s3')
    all_objects = s3_client.list_objects_v2(Bucket=landingBucketName, Prefix=landingFolder, MaxKeys=350)

    for obj in all_objects['Contents']:
        filePath = obj['Key']
        completeFileName = filePath.split('/')[-1]

        if len(completeFileName) > 0 and \
                (
                        completeFileName in validDataAndMetaDataFileDict.keys() or completeFileName in validDataAndMetaDataFileDict.values()):
            print('{} File is decrypted and moved to processing zone.'.format(completeFileName))
            landingKey = landingFolder + completeFileName
            processingKey = processingFolder + completeFileName
            archiveKey = archiveFolder + completeFileName

            decryption_class_obj = decryption_class(key_ids, landingBucketName, landingKey, bucketName, processingKey)
            #         Call the decryption method
            decryption_class_obj.decrypt_s3_file_method()

            copy_source = {
                'Bucket': landingBucketName,
                'Key': landingKey
            }
            s3_client.copy_object(Bucket=bucketName, Key=archiveKey, CopySource=copy_source)

            response = s3_client.delete_object(
                Bucket=landingBucketName,
                Key=landingKey,
            )


def send_sns_message(env,batch_region,rejected_file,process_name,reject_reason,reject_path):
    
    loggroupname = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
    Logstream = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
    
    
    sns_message = {
                  "Env": env,
                  "Batch_Region": batch_region,
                  "Rejected_File": rejected_file,
                  "Process_Name": process_name,
                  "Reject_Reason": reject_reason,
                  "Reject_Path": reject_path,
                  "Lambda_Name": os.environ['AWS_LAMBDA_FUNCTION_NAME'],
                  "Log Group": loggroupname,
                  "CloudWatch_Logstream": Logstream
    }
    
    sns_subject = 'Rejection in FileValidation'
    sns = boto3.client('sns')
    # acct = boto3.client('sts').get_caller_identity()['Account']
    snsarn = os.environ['sns_arn']
    snsMessage = json.dumps(sns_message)
    sns.publish(
        TargetArn=snsarn,
        Message=snsMessage,
        Subject=sns_subject
    )
    print('SNS Sent')
    return (0)


def connect_to_rds():
    try:
        rds_secret_name = os.environ['rds_secret_name']
        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(
            SecretId=rds_secret_name
            )
        
        print("rds secret details:-",response)
       
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("rds if")
            rds_database = ast.literal_eval(response['SecretString'])['engine']
            rds_port = ast.literal_eval(response['SecretString'])['port']
            rds_username = ast.literal_eval(response['SecretString'])['username']
            rds_password = ast.literal_eval(response['SecretString'])['password']
            rds_host = ast.literal_eval(response['SecretString'])['host']
            #rds_region = ast.literal_eval(response['SecretString'])['rds_region']
    
            rds_conn_string = "dbname='" + rds_database + "' port='" + str(rds_port) + "' user='" + rds_username + "' password='" + rds_password + "' host='" + rds_host + "'"
        else:
            print("Not Able to extract Credentials for RDS Connections")
            sys.exit('Not Able to extract Credentials for RDS Connections')
        
        print("calling rds connection")
        rds_connection = psycopg2.connect(rds_conn_string)
        return rds_connection

    except Exception as e:
        print("Exception caught while connecting to RDS.")
        print(traceback.format_exc())
        print(e)
        sys.exit(1)


# moving file in reject or processing zone
def moveFileToOtherFolder(bucketName, landingfolder, destinationFolder, receivedFileName, receivedMetadataFileName):
    s3_client = boto3.client('s3')
    sourceKey = landingfolder + receivedFileName
    destinationKey = destinationFolder + receivedFileName
    copy_source = {
        'Bucket': bucketName,
        'Key': sourceKey
    }
    s3_client.copy_object(Bucket=bucketName, Key=destinationKey, CopySource=copy_source)

    metadataSourceKey = landingfolder + receivedMetadataFileName
    metadataDestinationKey = destinationFolder + receivedMetadataFileName
    metadata_copy_source = {
        'Bucket': bucketName,
        'Key': metadataSourceKey
    }
    s3_client.copy_object(Bucket=bucketName, Key=metadataDestinationKey, CopySource=metadata_copy_source)

    response = s3_client.delete_objects(
        Bucket=bucketName,
        Delete={
            'Objects': [{'Key': sourceKey}, {'Key': metadataSourceKey}]
        }
    )
    return response


# move to await folder ..
def moveFileToWaitFolder(bucketName, landingfolder, destinationfolder, fileName):
    s3_client = boto3.client('s3')
    sourceKey = landingfolder + fileName
    destinationKey = destinationfolder + fileName
    copy_source = {
        'Bucket': bucketName,
        'Key': sourceKey
    }
    s3_client.copy_object(Bucket=bucketName, Key=destinationKey, CopySource=copy_source)

    response = s3_client.delete_objects(
        Bucket=bucketName,
        Delete={
            'Objects': [{'Key': sourceKey}]
        }
    )
    return response

def set_globals(rgn,intrvl,env):
    global pRegionName
    global Schedule
    global Environment
    global pScheduleType
    global Batch
    global bucketName
    global emrMasterScriptPath
    
    pRegionName = rgn
    Schedule = intrvl
    Environment = env
    pScheduleType = pRegionName + Schedule
    Batch = pRegionName + 'Batch'
    bucketName = 'sc360-' + Environment + '-' + pRegionName.lower() + '-bucket'
    emrMasterScriptPath = 's3://' + bucketName + '/Resources/Scripts/EC2MasterScript/EC2MasterPythonScript.py'
    
def validate_s3_folder_and_audit_log(bucket_name, folder_prefix, db_cursor, regionname):
    """
    Checks if the specified folder exists in the given S3 bucket.
    If not, checks the audit log table for:
      1. Recent failures in key processes today.
      2. Any 'ReadyToExecute' status in 'FileValidation' process for current batch.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_prefix (str): Prefix path to check (e.g., 'LandingZone/dt=2025-06-05/').
        db_cursor: A live DB cursor connected to the RDS instance.
        regionname: Region name.

    Returns:
        dict: {
            'folderExists': bool,
            'message': str,
            'auditFailure': bool (optional)
        }
    """
    s3 = boto3.client('s3')

    try:
        # Check if folder exists in S3
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix, MaxKeys=1)
        if 'Contents' in response:
            return {
                'folderExists': True,
                'message': "Folder exists in S3.",
                'auditFailure': False
            }
        else:
            print("S3 folder not found. Checking audit log for relevant entries.")

            # 1) Check for recent failures in critical processes today
            failure_check_sql = """
            SELECT  t1.filename,
                    t1.processname,
                    t1.executionstatus,
                    t1.executionendtime
            FROM audit.sc360_audit_log t1
            JOIN (
                SELECT processname, filename,
                       MAX(executionendtime) AS max_endtime
                FROM audit.sc360_audit_log
                WHERE processname IN ('RedshiftCuratedLoad', 'RedshiftPublishedLoad')
                  AND batchrundate = CURRENT_DATE
                  AND regionname = %s
                GROUP BY processname, filename
            ) t2
            ON t1.processname = t2.processname
            AND t1.filename = t2.filename
            AND t1.executionendtime = t2.max_endtime
            WHERE t1.executionstatus = 'Failed'
            AND t1.regionname = %s;
            """
            db_cursor.execute(failure_check_sql, (regionname, regionname))
            failed_runs = db_cursor.fetchall()
            print("Audit log failed_runs result:", failed_runs)

            if failed_runs and len(failed_runs) > 0:
                process_names = [row[1] for row in failed_runs]
                print(f"Recent failures found in processes: {process_names}. Allowing re-processing.")
                return {
                    'folderExists': True,
                    'message': f"Folder not found in S3, but recent process failure(s) detected for today: {process_names}. Allowing re-processing.",
                    'auditFailure': True
                }

            # 2) Check if any files are ReadyToExecute for FileValidation process today
            ready_to_execute_sql = """
            SELECT FileName, BatchId, DestinationName
            FROM audit.sc360_audit_log
            WHERE BatchRunDate = CURRENT_DATE
              AND ExecutionStatus = 'ReadyToExecute'
              AND ProcessName = 'FileValidation'
              AND regionname = %s;
            """
            db_cursor.execute(ready_to_execute_sql, (regionname,))
            ready_files = db_cursor.fetchall()
            print("ReadyToExecute files for FileValidation:", ready_files)

            if ready_files and len(ready_files) > 0:
                return {
                    'folderExists': True,
                    'message': f"No S3 folder found but found {len(ready_files)} FileValidation file(s) ReadyToExecute for current batch. Allowing processing.",
                    'auditFailure': True
                }

            # If none of the above conditions met, skip processing
            return {
                'folderExists': False,
                'message': f"No files found in S3 or audit logs for current batch in {folder_prefix}. Skipping further processing.",
                'auditFailure': False
            }

    except Exception as e:
        print("Error checking folder existence or querying audit log:")
        print(traceback.format_exc())
        return {
            'folderExists': False,
            'message': f"Failed during folder existence/audit log check: {str(e)}",
            'auditFailure': False
        }

import boto3
import time

def wait_for_glue_job_completion(glue_job_name, region_value='AMS', timeout_minutes=12, poll_interval=30):
    glue = boto3.client('glue')
    timeout_seconds = timeout_minutes * 60
    elapsed_time = 0

    print(f"Looking for Glue job runs with --region == '{region_value}' in job: {glue_job_name}")

    # Step 1: Fetch job runs and find the one with the latest timestamp for the given region
    response = glue.get_job_runs(JobName=glue_job_name, MaxResults=30)  # increased from 10 to 30
    matching_runs = [
        run for run in response['JobRuns']
        if run.get('Arguments', {}).get('--region') == region_value
    ]

    if not matching_runs:
        raise Exception(f"No Glue job run found with --region == '{region_value}'")

    # Pick the most recent one by StartedOn timestamp
    latest_run = max(matching_runs, key=lambda run: run['StartedOn'])
    target_run_id = latest_run['Id']
    print(f"Matching Glue job run found: {target_run_id} with status: {latest_run['JobRunState']} at {latest_run['StartedOn']}")

    # Step 2: Poll until job reaches terminal state
    while elapsed_time < timeout_seconds:
        run_status = glue.get_job_run(JobName=glue_job_name, RunId=target_run_id)
        state = run_status['JobRun']['JobRunState']
        print(f"Glue job run {target_run_id} status: {state}")

        if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            print(f"Glue job run {target_run_id} completed with status: {state}")
            return state

        time.sleep(poll_interval)
        elapsed_time += poll_interval

    raise TimeoutError(f"Glue job run {target_run_id} did not complete within {timeout_minutes} minutes")

def lambda_handler(event, context):
    # TODO implement
    try:
        rgn = event['region']
        intrvl = event['interval']
        env = os.environ['env']
        set_globals(rgn,intrvl,env)
        
        
        app_name = "my-master-job-py"
        ps_connection = connect_to_rds()  # connect to rds
        print("Connection Made")
        cursor = ps_connection.cursor()
        
        BatchRunDate = dt.date.today()
        BatchId = pRegionName + "Batch" + '_' + str(BatchRunDate.year) + str(BatchRunDate.month).zfill(2) + str(BatchRunDate.day).zfill(2)
        updateBatchid = 'Update audit.sc360_batch_master_updated set batchid = %s, batchrundate = %s where ScheduleType = %s;'  
        cursor.execute(updateBatchid, (BatchId,BatchRunDate,pScheduleType))
        BatchId = None
        BatchRunDate = None
        getBatchid = "select distinct batchid, batchrundate from audit.sc360_batch_master_updated where ScheduleType = %s ;"
        
        cursor.execute(getBatchid, (pScheduleType,))
        result = cursor.fetchall()
        print(result)
        if len(result) == 1:
            for row in result:
                print(row[0], row[1])
                tempBatchId = row[0]
                tempBatchRunDate = row[1]
        elif len(result) == 0:
            raise Exception("Invalid ScheduleType: " + pScheduleType)
        else:
            raise Exception("Multiple BatchId and BatchRundate combination. There should be only combination")

        if tempBatchId == None:
            todaysDate = dt.date.today()
            BatchId = Batch + '_' + str(todaysDate.year) + str(todaysDate.month).zfill(2) + str(
                todaysDate.day).zfill(2)
            BatchRunDate = todaysDate

            # updateBatchMaster = "UPDATE audit.sc360_batch_master_updated SET BatchId = %s, BatchRunDate = %s WHERE ScheduleType =%s;"
            # cursor.execute(updateBatchMaster, (BatchId, BatchRunDate, pScheduleType))

            ps_connection.commit()
        else:
            BatchId = tempBatchId
            BatchRunDate = tempBatchRunDate
        
        fileNameList = []
        originalFileNameList = []
        fileExtensionList = []
        fileSizeList = []
        metadataFileNameDict = {}

        landingBucketName = 'sc360-' + Environment + '-' + pRegionName.lower() + '-bucket'
        landingFolder = 'LandingZone/dt=' + str(BatchRunDate) + '/'
        processingFolder = 'ProcessingZone/dt=' + str(BatchRunDate) + '/'
        fileRejectFolder = 'RejectZone/FileRejectZone/dt=' + str(BatchRunDate) + '/'
        archiveFolder = 'ArchiveZone/dt=' + str(BatchRunDate) + '/'
        awaitFolder = 'ProcessingZoneTemp/dt=' + str(BatchRunDate) + '/'

        print('Bucket Name',landingBucketName)
        print('landingFolder',landingFolder)
        print('processingFolder',processingFolder)
        
        # Check if folder exists using helper
        folder_check = validate_s3_folder_and_audit_log(landingBucketName, landingFolder, cursor, pRegionName)
        if not folder_check['folderExists']:
            print(folder_check['message'])
            return folder_check
        
        if folder_check['auditFailure']:
            print(folder_check['message'])
            return folder_check
        
        s3 = boto3.client("s3")
        
        all_objects = s3.list_objects_v2(Bucket=landingBucketName, Prefix=landingFolder, MaxKeys=350)

        validDataAndMetaDataFileDict = {}
        dataFileNameList = []
        metadataFileNameList = []
        
        
        for obj in all_objects['Contents']:
            filePath = obj['Key']
            completeFileName = filePath.split('/')[-1]
            print('CompleteFile Name',completeFileName)
            if len(completeFileName) > 0 and not (completeFileName.startswith('SC360metadata_')):
                dataFileNameList.append(completeFileName)
            elif len(completeFileName) > 0 and completeFileName.startswith('SC360metadata_'):
                metadataFileNameList.append(completeFileName)
    
            
        print('dataFileNameList------->', dataFileNameList)
        print('metadataFileNameList------->', metadataFileNameList)
        
        dataFileAndBatchMasterDataResult = {}
        matchedMetaFileName = None
	#------------------------------------------------------------------------------------------------
        for dataFileName in dataFileNameList:
            matchedMetaFileName = None
            fileNameItems = dataFileName.split('_')
            fileNameKey = "_".join(fileNameItems[:len(fileNameItems) - 1])

            for item in metadataFileNameList:
                metaFileNameItems = item.split('_')
                metaFileNameKey = "_".join(metaFileNameItems[1:len(metaFileNameItems) - 1])
                # print('metaFileNameKey -->' + str(metaFileNameKey))
                # print('fileNameKey--->', str(fileNameKey))
                if metaFileNameKey == fileNameKey:
                    matchedMetaFileName = item
                    break

            matched_file_name = False
            for file_key in file_keys:
                if fileNameKey.startswith(file_key) or file_key in fileNameKey:
                    originalFileName = file_key
                    matched_file_name = True
                    break

            if matched_file_name == False:
                completeFileNameSplit = dataFileName.split('.')
                fileName = completeFileNameSplit[0]
                originalFileName = fileName[:len(fileName) - 15]
            
            print('Complete Original Name ', originalFileName)
            print('Going for BatchID Result----------------')
            getBatchMasterData = "select  DataPipelineName,RegionName,FileName,FileExtension from " \
                                 "audit.sc360_batch_master_updated where FileName = %s and ProcessName = %s and ScheduleType " \
                                 "= %s; "
            cursor.execute(getBatchMasterData, (originalFileName, ProcessName, pScheduleType))
            batchMasterDataResult = cursor.fetchone()

            if matchedMetaFileName == None or batchMasterDataResult == None:
                # move the file in waiting zone/TempZone
                if matchedMetaFileName == None:
                    # response = moveFileToWaitFolder(bucketName, landingFolder, awaitFolder, dataFileName)
                    print("No meta data file found , file moved to await zone")
                    
                # check for next file
            else:
                validDataAndMetaDataFileDict[dataFileName] = matchedMetaFileName
                dataFileAndBatchMasterDataResult[dataFileName] = batchMasterDataResult

	#-Loop ENDS---------------------------------------------------------------------------------------------------------
        print('Checking size and extension')
        print(validDataAndMetaDataFileDict)
        print("file_key and fileNameKey after the disctionary", file_key , fileNameKey)
        if len(validDataAndMetaDataFileDict) > 0:
            decrypt_and_archive(landingBucketName, landingFolder, bucketName, processingFolder, archiveFolder,
                                validDataAndMetaDataFileDict)

            all_objects = s3.list_objects_v2(Bucket=landingBucketName, Prefix=processingFolder, MaxKeys=350)
            for obj in all_objects['Contents']:
                filePath = obj['Key']
                # print('filePath', filePath)
                if filePath[-1] == '/':
                    continue

                splitFilePath = filePath.split('/')
                completeFileName = splitFilePath[-1]
                # print('completeFileName' + completeFileName,filePath)

                if len(completeFileName) > 0 and (
                        completeFileName in validDataAndMetaDataFileDict.keys() or completeFileName in validDataAndMetaDataFileDict.values()):
                    if (validDataAndMetaDataFileDict.keys() and completeFileName.startswith('SC360metadata_')):
                        metadataFileName = completeFileName[14:len(completeFileName) - 19]
                        print('Metadata file name is' , metadataFileName)
                        matched_file_name = False
                        originalName = None
                        for file_key in file_keys:
                            if metadataFileName.startswith(file_key) or file_key in metadataFileName:
                                originalName = file_key
                                matched_file_name = True
                                break
                        if matched_file_name == False:
                            metadataFileNameDict[metadataFileName] = completeFileName
                        else:
                            metadataFileNameDict[originalName] = completeFileName
                        
                        metadataFileNameList.append(metadataFileName)

                    #     Capturing Actual files from the LandingZone excluding folders
                    if (not (completeFileName.startswith('SC360metadata_'))):
                        completeFileNameSplit = completeFileName.split('.')
                        fileName = completeFileNameSplit[0]
                        matched_file_name = False
                        for file_key in file_keys:
                            if fileName.startswith(file_key) or  file_key in fileName:
                                originalFileName = file_key
                                matched_file_name = True
                                break
                        if matched_file_name == False:
                            originalFileName = fileName[:len(fileName) - 15]

                        fileExtension = completeFileNameSplit[1]
                        fileSize = obj['Size']
                        fileNameList.append(completeFileName)
                        originalFileNameList.append(originalFileName)
                        fileExtensionList.append(fileExtension)
                        fileSizeList.append(fileSize)
                else :
                    print(completeFileName + ' File is not found in current processing batch' )
            
            print('saved data file name', fileNameList)
            merged_list = list(tuple(zip(fileNameList, originalFileNameList, fileExtensionList, fileSizeList)))
            print('merged_list' + str(merged_list))
            
            for eachFile in merged_list:
                try:
                    receivedFileName = eachFile[0]
                    receivedOriginalFileName = eachFile[1]
                    receivedFileExtension = eachFile[2]
                    receivedFileSize = eachFile[3]
                    executionStatus = None
                    errorMessage = None
    
                    print('receivedOriginalFileName' + str(receivedOriginalFileName))
                    receivedMetadataFileName = metadataFileNameDict[
                        receivedOriginalFileName] if receivedOriginalFileName in metadataFileNameDict else None
                    print('receivedFileName' + str(receivedFileName))
                    print('dataFileAndBatchMasterDataResult' + str(dataFileAndBatchMasterDataResult))
    
                    batchMasterDataResults = dataFileAndBatchMasterDataResult[receivedFileName]
                    print('batchMasterDataResults' + str(batchMasterDataResults))
                    expectedFileExtension = batchMasterDataResults[3]
                    pDataPipelineName = batchMasterDataResults[0]
    
                    getSC360AuditData = "select  DataPipelineName,RegionName,FileName,ExecutionStatus from audit.sc360_audit_log where sourcename = %s and ProcessName = %s and BatchId = %s and ExecutionStatus IN ('ReadyToExecute','Submitted','InProgress','Succeeded');"
                    cursor.execute(getSC360AuditData, (receivedFileName, ProcessName, BatchId,))
                    sc360AuditDataResult = cursor.fetchone()
    
                    print('sc360AuditDataResult' + str(sc360AuditDataResult))
    
                    if sc360AuditDataResult == None and receivedOriginalFileName in metadataFileNameDict:
                        receivedMetadataFileName = metadataFileNameDict[receivedOriginalFileName]
                        
                        metafile = s3.get_object(
                            Bucket=bucketName,
                            Key=processingFolder + receivedMetadataFileName
                        )
    
                        metadataInfo = list(csv.reader(codecs.getreader("utf-8")(metafile["Body"])))
                        print("metadataInfo " + str(metadataInfo))
                        fileSizeFromSource = 0
                        print(metadataInfo[0][0])
                        if len(metadataInfo) > 0:
    
                            fileSizeFromSource = int(metadataInfo[0][0].split('|')[1])
    
                            print('fileSizeFromMeta---->', fileSizeFromSource)
                            print('ActualSize---->', receivedFileSize)
    
                            if receivedFileExtension.lower() == expectedFileExtension.lower():
    
                                if receivedFileSize == fileSizeFromSource:
                                    executionStatus = 'ReadyToExecute'
                                    # print("File comparison matches for " + receivedOriginalFileName)
                                    # move the file in proceessing folder
                                    # response = moveFileToOtherFolder(bucketName, landingFolder, processingFolder,
                                    #                                  receivedFileName, receivedMetadataFileName)
    
                                    print("File validation completed, execution Status--->", executionStatus)
    
                                else:
                                    executionStatus = 'FileValidationFailed'
                                    errorMessage = 'File size is not matched'
                                    # logging.error("Send Notification: FileSize didn't match for " + receivedFileName)
    
                            else:
                                executionStatus = 'FileValidationFailed'
    
                                if fileSizeFromSource == receivedFileSize:
                                    errorMessage = 'File extension is not matched'
                                    print("ERROR :- ", errorMessage)
                                    # logging.error(
                                    #     "Send Notification: File Extension didn't match for " + receivedFileName)
                                else:
                                    errorMessage = 'File size and extension are not matched'
                                    print("ERROR :- ", errorMessage)
                                    # logging.error(
                                    # "Send Notification: File Extension and FileSize didn't match for " + receivedFileName)
                        else:
                            # logging.error("Meta data file is not valid" + receivedFileName)
                            executionStatus = 'FileValidationFailed'
                            errorMessage = 'Meta data file is not valid'
                            print("ERROR :- ", errorMessage)
    
                        log_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                        rds_insert_query = """
    
                            INSERT INTO audit.sc360_audit_log(
                                batchid, batchrundate, regionname, datapipelinename, pipelinerunid, processname,
                                filename, sourcename, destinationname, sourceplatform, targetplatform, activity,
                                environment, interval, scriptpath, executionstatus, errormessage, datareadsize,
                                datawrittensize, logtimestamp)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );
                            """
    
                        records_insert = (
                            BatchId, BatchRunDate, pRegionName, pDataPipelineName, pDataPipelineId
                            , ProcessName, receivedOriginalFileName, receivedFileName, receivedFileName
                            , SourcePlatform, TargetPlatform, Activity, Environment, pScheduleType,
                            emrMasterScriptPath
                            , executionStatus, errorMessage, fileSizeFromSource, receivedFileSize, log_timestamp)
                        cursor = ps_connection.cursor()
                        cursor.execute(rds_insert_query, records_insert)
                        ps_connection.commit()
                        print("Insert Done")
    
                        if errorMessage != None:
                            rejection_file_path = urlparse('s3://' + fileRejectFolder + receivedFileName)
                            # app_name = "SC360"
                            print("Moving file", receivedFileName, "to reject file zone")
                            response = moveFileToOtherFolder(bucketName, processingFolder, fileRejectFolder,
                                                             receivedFileName,
                                                             receivedMetadataFileName)
                            send_sns_message(Environment,pRegionName,receivedFileName,ProcessName,errorMessage, (rejection_file_path).geturl())
                except Exception as e:
                    print('Some error occured while proceessing...moving to next file.')

        else:
            print('No valid file to process')
        
        # checking data validation glue job status
        try:
            job_status = wait_for_glue_job_completion(os.environ['datavalidation_glue_job'], region_value=pRegionName)
            print(f"Glue job completed with status: {job_status}")
        except Exception as e:
            print(f"Error while waiting for Glue job: {str(e)}")
            raise e

        # If execution reached here, folder existed and Lambda ran normally
        return {
            'folderExists': True,
            'message': 'Files validated and processed.'
        }

    except Exception as e:
        print('Error occurred: ', e)
        return {
            'folderExists': False,
            'message': f"Execution failed: {str(e)}"
        }
