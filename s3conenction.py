import boto3
import os
import json
import datetime
from datetime import date


def claim_handler(event,context):
    print(f"event details are ....{event}")
    #creating s3 client
    s3=boto3.client("s3")
    #Manually defing the Destination Bucket
    destination_bucket="data_dest"
    print("Destination Bucket is {0}".format(destination_bucket))
    
    source_bucket=event['Records'][0]['s3']['bucket']['name']
    print(f"source bucket name is {source_bucket}")

    #getting source key
    source_key=event['Records'][0]['s3']['object']['key']
    print("source file name id {0}".format(source_key))

    #Deleteing the destination file
    objects = s3.list_objects_v2(Bucket=destination_bucket)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            key = obj['Key']  # Getting file key
            # Deleting the object 
            s3.delete_object(Bucket=destination_bucket, Key=key)



   
    #copying the source file to target
    source_file = {
        'Bucket': source_bucket,
        'Key': source_key
    }

    s3.copy_object(
        CopySource=source_file,
        Bucket=destination_bucket,
        Key=source_key
    )
    
    
    
    
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
                # Iterate through each object
                for obj in objects['Contents']:
                    key = obj['Key']
                    # Check if the object key matches the specific file we want to delete
                    if key == "some_file":
                        # Delete the specific file
                        s3.delete_object(Bucket=bucket, Key=key)
