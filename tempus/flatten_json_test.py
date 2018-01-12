
from pprint import pprint
import os
import json
import urllib
import csv

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
        with open(csv_filename, 'a') as f:
            csv_writer = csv.DictWriter(f, sorted(dict_data.keys()))
            if (write_header):
                csv_writer.writeheader()
            csv_writer.writerow(dict_data)
    except IOError as (errno, strerror):
            print("I/O error({0}): {1}".format(errno, strerror))

input_json = 'patient-examples-general.json'
json_data = {}

json_object = json.load(open(input_json))
flattened_data = {}
write_header = True

if 'resourceType' in json_object and json_object['resourceType'] == 'Bundle':
    for entry in json_object['entry']:
        flattened_data = {}
        flattened_data = flatten_json(entry['resource'], '.')
        flattened_data['fullUrl'] = entry['fullUrl']
        write_dict_to_csv('out.csv', flattened_data, write_header)
        write_header = False
else:
    flattened_data = flatten_json(json_object, '.')
    write_dict_to_csv('out.csv', flattened_data, write_header)
