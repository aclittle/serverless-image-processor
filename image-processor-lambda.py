import boto3
import json
import logging
import os
from io import BytesIO
from PIL import Image
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Configuration
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
MAX_SIZE = 1024  # maximum dimension for resized image

def resize_image(image_body):
    with Image.open(BytesIO(image_body)) as img:
        img.thumbnail((MAX_SIZE, MAX_SIZE))
        buffer = BytesIO()
        img.save(buffer, format=img.format)
        buffer.seek(0)
        return buffer

def lambda_handler(event, context):
    try:
        # Get the object from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Download the image from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        image_body = response['Body'].read()
        
        # Resize the image
        resized_image = resize_image(image_body)
        
        # Upload the resized image to the output bucket
        output_key = f"resized-{key}"
        s3_client.put_object(Bucket=OUTPUT_BUCKET, Key=output_key, Body=resized_image.getvalue())
        
        # Save metadata to DynamoDB
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(
            Item={
                'image_id': key,
                'original_bucket': bucket,
                'original_key': key,
                'resized_bucket': OUTPUT_BUCKET,
                'resized_key': output_key,
                'timestamp': datetime.now().isoformat()
            }
        )
        
        # Send SNS notification
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({
                'message': 'Image processed successfully',
                'original_image': f"{bucket}/{key}",
                'resized_image': f"{OUTPUT_BUCKET}/{output_key}"
            }),
            Subject='Image Processing Completed'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Image processed successfully')
        }
    
    except Exception as e:
        logger.error(f"Error processing image: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing image')
        }
