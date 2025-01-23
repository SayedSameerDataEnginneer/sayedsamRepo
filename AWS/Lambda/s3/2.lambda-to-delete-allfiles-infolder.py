import boto3
import json
import datetime 
from datetime import date

def claim_handler(event, context):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Iterate through all S3 buckets
    for buck in s3.list_buckets()['Buckets']:
        bucket = buck["Name"]
        # Check if the bucket is the one we need
        if bucket == "some_bucket_i_required":
            # List objects in the specified bucket
            objects = s3.list_objects_v2(Bucket=bucket)
            if 'Contents' in objects:
                #Iterate through each object
                for obj in objects['Contents']:
                    key = obj['Key']
                    #Deletimg_all_files_in_folder
                    s3.delete_object(Bucket=bucket, Key=key)
 