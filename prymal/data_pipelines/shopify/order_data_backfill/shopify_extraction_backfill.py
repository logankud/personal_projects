import boto3
import base64
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError
import json
import requests
import pandas as pd
import datetime
from datetime import timedelta
from pytz import timezone
import pytz
import os
import psycopg2
import loguru
from loguru import logger
import re
import io
import time

# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'

start_time = datetime.datetime.now()
logger.info(f'Start time: {start_time}')

SHOPIFY_API_KEY = os.environ['SHOPIFY_API_KEY']
SHOPIFY_API_PASSWORD = os.environ['SHOPIFY_API_PASSWORD']

API_VERSION = '2021-07'
START_DATE = pd.to_datetime(pd.to_datetime('2024-10-01') - timedelta(hours=5)).strftime('%Y-%m-%d 00:00:00')
END_DATE = pd.to_datetime(pd.to_datetime('today') - timedelta(days=1,hours=5)).strftime('%Y-%m-%d 23:59:59')
# END_DATE = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d 23:59:59')

SHOPIFY_URL = f'https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@prymal-coffee-creamer.myshopify.com/admin/api/{API_VERSION}'

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


# --------------------------------------------------------------------------------------

# Set dates to pull data for
sdate = pd.to_datetime(START_DATE)
edate = pd.to_datetime(END_DATE)

# Format API and pull all data from desired date range:

# Set timezone - Pacific for shopify data
pacific = timezone('US/Pacific')

# extract year, month and date from user input
y_start = edate.strftime('%Y')
m_start = edate.strftime('%m')
d_start = edate.strftime('%d')

# Set the start_date (which is the most current date of data we want)
start_date = pacific.localize(
  datetime.datetime(int(y_start), int(m_start), int(d_start), 23, 59,
                    59))  # datetime.date(%Y,%m,%d,%H,%M,%S)

# extract year, month and date from user input
y_end = sdate.strftime('%Y')
m_end = sdate.strftime('%m')
d_end = sdate.strftime('%d')

# Set the end_date (which is the oldest date of data we want, where the loop will terminate)
end_date = pacific.localize(
  datetime.datetime(int(y_end), int(m_end), int(d_end), 0, 0,
                    0))  # datetime.date(%Y,%m,%d,%H,%M,%S)

logger.info('backfill start date: ', START_DATE)
logger.info('backfill end date: ', END_DATE)
logger.info('shopify api data being pulled from: ', sdate, ' through ', edate)

# Create temp lists to hold the values in the json file
line_items = []
prices = []
order_id = []
created_at = []
email = []
subtotal_price = []
total_tax = []
total_discounts = []
financial_status = []
line_item_qty = []
shipping_fees = []

# Set paramaters for GET request
payload = {
  'limit': 250,
  'created_at_max': start_date,
  'created_at_min': end_date,
  'financial_status': 'paid'
}

# Blank df to store line item details
shopify_line_item_df = pd.DataFrame()
shopify_orders_df = pd.DataFrame()

# Shopify API URL and endpoint
url = f'https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@prymal-coffee-creamer.myshopify.com/admin/api/2021-07/orders.json?status=any'
has_next_page = True

max_retries = 3

while has_next_page == True:

  time.sleep(2)

  for retry in range(max_retries):

    try:
 
      r = requests.get(url, stream=True, params=payload)

      r.raise_for_status()  # Check for any HTTP errors

      if r.status_code == 200:
        response_json = r.json()
        break
      
    except requests.exceptions.HTTPError as errh:
      logger.info("HTTP Error:", errh)
      time.sleep(30)
      continue

    except requests.exceptions.ConnectionError as errc:
      logger.info("Error Connecting:", errc)
      time.sleep(30)
      continue 

    except requests.exceptions.Timeout as errt:
      logger.info("Timeout Error:", errt)
      time.sleep(30)
      continue

    except requests.exceptions.RequestException as err:
      logger.info("Error:", err)
      time.sleep(30)
      continue

    if retry < max_retries - 1:
       logger.info('Retrying api call...')
    else:
       logger.info(f'Retried {max_retries} times without getting a 200 response.')



  # --------------------------- ORDER DF ----------------------------

  # Normalize Shopify Orders
  orders = pd.json_normalize(response_json['orders'])

  # Select relevant columns
  orders_df = orders[[
    'order_number', 'email', 'created_at', 'shipping_address.address1',
    'shipping_address.city', 'shipping_address.province',
    'shipping_address.country', 'subtotal_price', 'total_line_items_price',
    'total_tax', 'total_discounts',
    'total_shipping_price_set.shop_money.amount', 'total_price'
  ]].copy()
  # Rename columns
  orders_df.columns = [
    'order_id', 'email', 'created_at', 'shipping_address', 'shipping_city',
    'shipping_province', 'shipping_country', 'subtotal_price',
    'total_line_items_price', 'total_tax', 'total_discounts',
    'total_shipping_fee', 'total_price'
  ]

  logger.info(f"Min order date: {orders_df['created_at'].min()}")

  # Use created_at to create a formatted date column (yyyy-mm-dd)
  orders_df['order_date'] = pd.to_datetime(orders_df['created_at'].str.slice(0, 19),format='%Y-%m-%dT%H:%M:%S').dt.strftime('%Y-%m-%d')

  shopify_orders_df = pd.concat([shopify_orders_df, orders_df])

  # --------------------------- LINE ITEM DF ----------------------------

  # Iterate through orders_df and normalize line_items_df
  for i in range(len(response_json['orders'])):

    line_items = pd.json_normalize(response_json['orders'][i]['line_items'])

    line_items = line_items[[
      'name', 'price', 'quantity', 'sku', 'title', 'variant_title'
    ]].copy()

    line_items['order_id'] = orders.iloc[i]['order_number']
    line_items['email'] = orders.iloc[i]['email']
    line_items['created_at'] = orders.iloc[i]['created_at']

    line_items['order_date'] = pd.to_datetime(
      line_items['created_at'].str.slice(0, 19),format='%Y-%m-%dT%H:%M:%S').dt.strftime('%Y-%m-%d')

    line_items.columns = [
      'line_item_name', 'price', 'quantity', 'sku', 'title', 'variant_title',
      'order_id', 'email', 'created_at', 'order_date'
    ]
    line_items = line_items[[
      'order_id', 'email', 'created_at', 'order_date', 'price', 'quantity',
      'sku', 'title', 'variant_title', 'line_item_name'
    ]].copy()

    line_items.reset_index(inplace=True, drop=True)

    shopify_line_item_df = pd.concat([shopify_line_item_df, line_items])

  # Paginate results if there are more than 250 orders
  if 'link' in r.headers and 'rel="next"' in r.headers['link']:

    if len(r.headers['link'].split(',')) > 1:
      next_link = r.headers['link'].split(',')[1].split('>')[0].replace(
        '<', '')
      url = f'{next_link.split("//")[0]}' + f'//{SHOPIFY_API_KEY}:' + f'{SHOPIFY_API_PASSWORD}' + '@' + f'{next_link.split("//")[1]}'

    else:
      next_link = r.headers['link'].split('>')[0].replace('<', '')
      url = f'{next_link.split("//")[0]}' + f'//{SHOPIFY_API_KEY}:' + f'{SHOPIFY_API_PASSWORD}' + '@' + f'{next_link.split("//")[1]}'

  else:
    has_next_page = False

  #Reset Payload
  payload = {'limit': 250}

  logger.info(orders_df['order_date'].min(), orders_df['order_date'].max())
  logger.info(f'Has next page: {has_next_page}')
  logger.info(f'Total orders: {len(shopify_orders_df)}')
  logger.info(
    f'Orders date range: {shopify_orders_df["order_date"].min()} to {shopify_orders_df["order_date"].max()}'
  )
  logger.info(f'Total line items: {len(shopify_line_item_df)}')
  logger.info(
    f'Line items date range: {shopify_line_item_df["order_date"].min()} to {shopify_line_item_df["order_date"].max()}'
  )

shopify_line_item_df.reset_index(inplace=True, drop=True)
shopify_orders_df.reset_index(inplace=True, drop=True)

current_time = datetime.datetime.now()
logger.info(f'Current time: {current_time}')
logger.info(f'Elaspsed time: {current_time - start_time}')

# CONFIGURE BOTO  =======================================



AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']


# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL']

# WRITE ORDERS TO S3 =======================================

current_time = datetime.datetime.now()
logger.info(f'Current time: {current_time}')

logger.info(f'{len(shopify_orders_df)} rows in shopify_orders_df')

# Write each date's data to the appopriate s3 partition 
for date in pd.to_datetime(shopify_orders_df['order_date']).unique():
   
  ORDER_DATE = pd.to_datetime(date).strftime('%Y-%m-%d')
  ORDER_DATE_Y = pd.to_datetime(date).strftime('%Y')
  ORDER_DATE_M = pd.to_datetime(date).strftime('%m')
  ORDER_DATE_D = pd.to_datetime(date).strftime('%d')

  df = shopify_orders_df.loc[shopify_orders_df['order_date']==ORDER_DATE].copy()

  logger.info(f'Record count for shopify_orders {ORDER_DATE} - {len(df)}')

  # Configure S3 Prefix
  S3_PREFIX_PATH = f"shopify/orders/year={ORDER_DATE_Y}/month={ORDER_DATE_M}/day={ORDER_DATE_D}/shopify_orders_{ORDER_DATE}.csv"

  # Check if data already exists for this partition
  data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

  # If data already exists, delete it .. 
  if data_already_exists == True:
    
    # Delete data 
    delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


  logger.info(f'Writing to {S3_PREFIX_PATH}')

  with io.StringIO() as csv_buffer:
      df.to_csv(csv_buffer, index=False)

      response = s3_client.put_object(
          Bucket=BUCKET, 
          Key=S3_PREFIX_PATH, 
          Body=csv_buffer.getvalue()
      )

      status = response['ResponseMetadata']['HTTPStatusCode']

      if status == 200:
          logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
      else:
          logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")



# WRITE LINE ITEMS TO S3 =======================================


current_time = datetime.datetime.now()
logger.info(f'Current time: {current_time}')


logger.info(f'{len(shopify_line_item_df)} rows in shopify_line_item_df')

# Write each date's data to the appopriate s3 partition 
for date in pd.to_datetime(shopify_line_item_df['order_date']).unique():
   
  ORDER_DATE = pd.to_datetime(date).strftime('%Y-%m-%d')
  ORDER_DATE_Y = pd.to_datetime(date).strftime('%Y')
  ORDER_DATE_M = pd.to_datetime(date).strftime('%m')
  ORDER_DATE_D = pd.to_datetime(date).strftime('%d')

  df = shopify_line_item_df.loc[shopify_line_item_df['order_date']==ORDER_DATE].copy()

  logger.info(f'Record count for shopify_line_items {ORDER_DATE} - {len(df)}')


  # Configure S3 Prefix
  S3_PREFIX_PATH = f"shopify/line_items/year={ORDER_DATE_Y}/month={ORDER_DATE_M}/day={ORDER_DATE_D}/shopify_line_items_{ORDER_DATE}.csv"

  # Check if data already exists for this partition
  data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

  # If data already exists, delete it .. 
  if data_already_exists == True:
    
    # Delete data 
    delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

  logger.info(f'Writing to {S3_PREFIX_PATH}')


  with io.StringIO() as csv_buffer:
      df.to_csv(csv_buffer, index=False)

      response = s3_client.put_object(
          Bucket=BUCKET, 
          Key=S3_PREFIX_PATH, 
          Body=csv_buffer.getvalue()
      )

      status = response['ResponseMetadata']['HTTPStatusCode']

      if status == 200:
          logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
      else:
          logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")

