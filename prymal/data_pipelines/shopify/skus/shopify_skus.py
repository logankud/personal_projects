

import boto3
import base64
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError
import pandas as pd
import datetime
from datetime import timedelta
import os
import loguru
from loguru import logger
import io
import time

# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'
CRAWLER_NAME = 'skus_shopify'

S3_PREFIX_TO_WRITE_TO = 'skus/shopify'
FILE_NAME_PREFIX = 'skus_shopify_'

# AWS Credentials
AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']
# Set bucket
BUCKET = os.environ['S3_PRYMAL']

# -------------------------------------
# Functions
# -------------------------------------

# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

    logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

    try:

        # Create s3 client
        s3_client = boto3.client('s3', 
                                region_name = REGION,
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        # List objects in s3_prefix
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

        # Instantiate objects_exist
        objects_exist=False

        # Set objects_exist to true if objects are in prefix
        if 'Contents' in result:
            objects_exist=True

            logger.info('Data already exists!')

        return objects_exist

    except NoCredentialsError:
                # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"The specified object key does not exist: {e}")
        else:
            logger.error(f"AWS S3 Error: {e}")

    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


    logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

    # Create an S3 client
    s3_client = boto3.client('s3', 
                            region_name = REGION,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
                                                                                
        # Use list_objects_v2 to list all objects within the specified prefix
        objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

        # Extract the list of object keys
        keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

        # Check if there are objects to delete
        if keys_to_delete:
            # Delete the objects using 'delete_objects'
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
            )
            logger.info(f"Deleted {len(keys_to_delete)} objects")
        else:
            logger.info("No objects to delete")

        return response

    except NoCredentialsError:
                # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"The specified object key does not exist: {e}")
        else:
            logger.error(f"AWS S3 Error: {e}")

    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")


# Write to an S3 Path
# -----------

def write_df_to_s3(df:pd.DataFrame,bucket:str, s3_prefix:str):

    logger.info(f'Writing to {s3_prefix}')

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)


        try:

            # Create an S3 client
            s3_client = boto3.client('s3', 
                                    region_name = REGION,
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                
            response = s3_client.put_object(
                Bucket=bucket, 
                Key=s3_prefix, 
                Body=csv_buffer.getvalue()
            )

            status = response['ResponseMetadata']['HTTPStatusCode']

            if status == 200:
                logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
            else:
                logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")

            return status

        except NoCredentialsError:
             # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

        except PartialCredentialsError as e:
            # Handle incomplete AWS credentials
            logger.error(f"Partial AWS credentials error: {e}")

        except ClientError as e:
            # Handle S3-specific errors
            if e.response['Error']['Code'] == 'NoSuchBucket':
                logger.error(f"The specified bucket does not exist: {e}")
            elif e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"The specified object key does not exist: {e}")

            else:
                logger.error(f"AWS S3 Error: {e}")



        except BotoCoreError as e:
            # Handle general BotoCore errors (e.g., network issues)
            logger.error(f"BotoCore Error: {e}")

        except Exception as e:
            # Handle other exceptions
            logger.error(f"Other Exception: {e}")



# Function to run glue crawlers
# -----------

def run_glue_crawler(crawler_name:str):

    logger.info(f'Running glue crawler: {crawler_name}')

    # Create an AWS Glue client
    glue_client = boto3.client('glue', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        # Trigger the Crawler run using the 'start_crawler' method
        response = glue_client.start_crawler(Name=crawler_name)

        logger.info(f"Crawler {crawler_name} started successfully.")

    except NoCredentialsError:
             # Handle missing AWS credentials
            logger.error("No AWS credentials found. Please configure your credentials.")

    except PartialCredentialsError as e:
        # Handle incomplete AWS credentials
        logger.error(f"Partial AWS credentials error: {e}")

    except ClientError as e:
        # Handle S3-specific errors
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"The specified bucket does not exist: {e}")
        elif e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"The specified object key does not exist: {e}")
        # log CrawlerRunningException error
        elif e.response['Error']['Code'] == 'CrawlerRunningException':
            logger.info(f"Crawler {crawler_name} is already running.. unable to trigger a new run at this time")

        else:
            logger.error(f"AWS S3 Error: {e}")



    except BotoCoreError as e:
        # Handle general BotoCore errors (e.g., network issues)
        logger.error(f"BotoCore Error: {e}")

    except Exception as e:
        # Handle other exceptions
        logger.error(f"Other Exception: {e}")

    # Return response
    return response

# --------------------------------------------------------------------------------------

# -------------------------------------
# EXECUTE SCRIPT
# -------------------------------------


CURRENT_DATE = pd.to_datetime('today').strftime('%Y-%m-%d')

# Read current shopify_skus.csv file into memory
df = pd.read_csv('shopify_skus.csv')

logger.info(f'Publishing shopify_skus.csv as new partition in prymal_shopify_skus Glue table')

# Configure S3 Prefix
S3_PREFIX_PATH = f"{S3_PREFIX_TO_WRITE_TO}/load_date={CURRENT_DATE}/{FILE_NAME_PREFIX}{CURRENT_DATE}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:

    # Delete data 
    response = delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

    # If data was successfully deleted
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.error(f'ERROR when deleting objects from {S3_PREFIX_PATH}')
    

# Write pandas dataframe to specified S3 path
response = write_df_to_s3(df=df, bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# Log response
logger.info(f'Response from writing to {S3_PREFIX_PATH} - {response}')

# If successful write, run glue crawler to update table metadata
if response['ResponseMetadata']['HTTPStatusCode'] == 200:

    # Run crawler
    run_glue_crawler(crawler_name=CRAWLER_NAME)
