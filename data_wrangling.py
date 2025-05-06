#!/usr/bin/env python
# coding: utf-8

# # Data wrangling via AWS Lambda

# In[ ]:


import boto3
import requests

s3 = boto3.client("s3")

def lambda_handler(event,context):
    # obtain data
    api_url = "https://api.realestate.com.au/prices/v1/realestate-properties"
    response = requests.get(api_url,params={"limit":1000})
    raw_data = response.json()
    # Write to S3 by date partition
    upload_path = f"raw/mel_housing/year={2025}/month=03/day=06/data.json"
    s3.put_object(
        Bucket="etl-bucket",
        Key=upload_path,
        Body=json.dumps(raw_data)
    )

