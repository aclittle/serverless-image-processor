import json
import boto3
import os
from PIL import Image
import io
import uuid
import time

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Download the image from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_content = response['Body'].read()
        
        # Open the image using Pillow
        img = Image.open(io.BytesIO(image_content))
        
        # Process the image (example: resize to 100x100)
        img.thumbnail((100, 100))
        
        # Save the processed image
        buffer = io.BytesIO()
        img.save(buffer, format=img.format)
        buffer.seek(0)
        
        # Upload the processed image back to S3
        processed_key = f"processed/{os.path.basename(key)}"
        s3.put_object(Bucket=bucket, Key=processed_key, Body=buffer)
        
        # Save metadata to DynamoDB
        image_id = str(uuid.uuid4())
        timestamp = int(time.time())
        item = {
            'ImageId': image_id,
            'FileName': key,
            'ProcessedFileName': processed_key,
            'Status': 'Processed',
            'UploadDate': timestamp,
            'ProcessedDate': timestamp
        }
        table.put_item(Item=item)
        
        # Send SNS notification
        message = f"Image {key} has been processed successfully."
        sns.publish(TopicArn=sns_topic_arn, Message=message)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Image processed successfully')
        }
    except Exception as e:
        print(f"Error processing image: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing image: {str(e)}')
        }
