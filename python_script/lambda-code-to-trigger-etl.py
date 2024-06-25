#This script sets up a logging environment, imports necessary modules, and defines a 
# ...Lambda function that triggers an AWS Glue job. The function logs the event details and 
# ...the Glue job run ID. Please replace "MyTestJob" with your actual Glue job name. 
# ...This script is typically used in AWS Lambda to trigger AWS Glue jobs based on certain events. 
# ...Make sure to configure your AWS credentials and permissions appropriately to run this script.
import json
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

# Variables for the job:
glueJobName = "MyTestJob"

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT: ')
    logger.info(event['detail'])
    response = client.start_job_run(JobName = glueJobName)
    logger.info('## STARTED GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response
