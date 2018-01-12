#!/usr/bin/env python
# Copyright 2016 Amazon.com, Inc. or its
# affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License is
# located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
import os
import json
import urllib
import boto3
import logging
import csv

logging.basicConfig(filename='output.log',level=logging.DEBUG)

input_bucket_name = os.environ['s3InputBucket']
output_bucket_name = os.environ['s3OutputBucket']
sqsqueue_name = os.environ['SQSBatchQueue']
aws_region = os.environ['AWSRegion']
s3 = boto3.client('s3', region_name=aws_region)
sqs = boto3.resource('sqs', region_name=aws_region)

def get_messages_from_sqs():
    results = []
    queue = sqs.get_queue_by_name(QueueName=sqsqueue_name)
    for message in queue.receive_messages(VisibilityTimeout=120,
                                          WaitTimeSeconds=20,
                                          MaxNumberOfMessages=10):
        results.append(message)
    return(results)


def format_string(input_string):
    if input_string and isinstance(input_string, basestring):
        # Remove newlines
        input_string = input_string.replace('\n', " ")
        input_string = input_string.replace(',', " ")
    return input_string


def flatten_json( json_data, key_suffix ):
    result = {}
    for key in json_data.keys():
        if isinstance( json_data[key], dict ):
            get = flatten_json( json_data[key], key_suffix )
            for j in get.keys():
                result[ key + key_suffix + j ] = format_string(get[j])
        elif isinstance(json_data[key], list):
            index=0
            for element in json_data[key]:
                if isinstance( element, dict ):
                    output = flatten_json( element, key_suffix )
                    for out_key in output:
                        result[key + key_suffix + str(index) + key_suffix + out_key] = format_string(output[out_key])
                else:
                    result[key + key_suffix + str(index)] = format_string(element)
                index = index + 1
        else:
            result[key] = format_string(json_data[key])
    return result


def write_dict_to_csv(csv_filename, dict_data, write_header):
    try:
        logging.debug('Writing to csv file ' + csv_filename)
        with open(csv_filename, 'a') as f:
            csv_writer = csv.DictWriter(f, sorted(dict_data.keys()))
            if (write_header):
                csv_writer.writeheader()
            csv_writer.writerow(dict_data)
    except IOError as (errno, strerror):
        logging.warning("I/O error({0}): {1}".format(errno, strerror))


def process_json():
    """
        Process Json File

        No real error handling in this sample code. In case of error we'll put
        the message back in the queue and make it visable again. It will end up in
        the dead letter queue after five failed attempts.
    """
    logging.debug('Processing JSON')
    for message in get_messages_from_sqs():
        try:
            # Read file name from SQS and download JSON file
            message_content = json.loads(message.body)
            input_json = urllib.unquote_plus(message_content
                                                ['Records'][0]['s3']['object']
                                                ['key']).encode('utf-8')
            s3.download_file(input_bucket_name, input_json, input_json)
            # Load JSON file and flatten structure
            json_data = {}
            json_object = json.load(open(input_json))
            flattened_data = {}
            write_header = True
            output_filename = os.path.basename(input_json) + '.csv'
            if 'resourceType' in json_object and json_object['resourceType'] == 'Bundle':
                for entry in json_object['entry']:
                    flattened_data = flatten_json(entry['resource'], '.')
                    flattened_data['fullUrl'] = entry['fullUrl']
                    write_dict_to_csv(output_filename, flattened_data, write_header)
                    write_header = False
            else:
                flattened_data = flatten_json(json_object, '.')
                write_dict_to_csv(output_filename, flattened_data, write_header)
            # write CSV to S3
            s3.upload_file('/' + output_filename,
                           output_bucket_name, output_filename)
            # remove file
            os.remove(input_json)
            os.remove(output_filename)
        except:
            message.change_visibility(VisibilityTimeout=0)
            continue
        else:
            message.delete()


def main():
    while True:
        process_json()


if __name__ == "__main__":
    main()
