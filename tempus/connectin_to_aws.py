import os
import json
import urllib
import boto3
from boto3.session import Session

key_id='AKIAJFCKUGNVR5CVBJOA'
secret='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'

session = Session(aws_access_key_id=key_id, aws_secret_access_key=secret, region_name='us-east-1')
s3 = session.resource("s3")

sqs = boto3.resource('sqs', 'us-east-1')
sqsqueue_name='SQSDeadLetterQueue'
queue = sqs_client.get_queue_by_name(QueueName=sqsqueue_name)

sqs_client = boto3.client('sqs', aws_access_key_id=key_id, aws_secret_access_key=secret, region_name='us-east-1')
