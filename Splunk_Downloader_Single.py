#!/usr/bin/env python3
# coding: utf-8

## Downloads data from Splunk and writes to an S3 bucket


import boto3
import json
import pandas as pd
import time

from io import StringIO

import splunklib.client as spclient


# AWS: Output Configuration
s3_bucket = 'splunk-export-to-s3'
s3_base_key = 'inbox/bot_signal_raw/'
aws_region_name = 'us-west-2'


# Splunk: API Configuration
splunk_api_token_name = 'splunk_api_token'
HOST = 'es.splk.me'
PORT = 8089
splunk_time_format = '%Y-%m-%dT%H:%M:%S.%f'
max_count = 12345678   # Maximum number of events allowed to be returned from the Splunk API

# Splunk: Time Range Configuration
start_time_str = '2022-10-21 00:00'
start_time_region = 'us/pacific' #'utc'
range_periods = 10  # Number of time periods to generate.
range_freq = '1h'  # Date/time period length for each exported file. #5min #1h #1d
use_sampling = False


# Splunk: Query Configuration
# splunk_query = 'search index=summary_cisbot sourcetype=stash signal=*'
splunk_query = 'search index=_internal sourcetype=splunkd'

# Sampling Logic:
# for high data volumes, 
#     shorten the frequency (more files, smaler size)
#     use sampling (smaller size, incomplete data)
if use_sampling:
    file_name_template = 'bot_signal_raw_<ts>_<freq>_sampled_<ratio>.json'
    sample_ratio=2
if not use_sampling:
    file_name_template = 'bot_signal_raw_<ts>_<freq>.json'
    sample_ratio=1


# AWS Simple Systems Manager / BOTO Session Setup
aws = boto3.Session(region_name=aws_region_name)
ssm = aws.client('ssm')
s3_resource = aws.resource('s3')
s3_client = aws.client('s3')


# Splunk SDK Session setup w/ SSM Params
# Comment out the line below and set splunk_token directly to
#  avoid using AWS SSM.
splunk_param = ssm.get_parameter(Name=splunk_api_token_name, WithDecryption=True)
splunk_token = splunk_param['Parameter']['Value']

# Start time in both specific TZ and UTC
start_time = pd.Timestamp(start_time_str, tz=start_time_region)
start_time_utc = start_time.astimezone('utc')


# Open Splunk API session (only once)
service = spclient.connect(host=HOST,port=PORT,token=splunk_token)



# 
# Main data export loop
#
timer_start = time.time()
for dt in pd.date_range(start=start_time_utc, periods=range_periods, freq=range_freq):

    # Construct the filename with timestamp, frequency, and sampling ratio.
    ts = dt.strftime('%Y%m%d%H%M')  # timestamp
    filename = file_name_template.replace('<ts>',ts).replace('<freq>',range_freq)
    if use_sampling:
        filename = filename.replace('<ratio>',str(sample_ratio))
    
    # Check if file exists in S3, if yes print message and move on.
    key = s3_base_key + f'{dt.year}/{dt.month:02d}/{dt.day:02d}/'+filename
    result = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=key)
    if 'Contents' in result:
        fsize = result['Contents'][0]['Size']
        print(f'{key} exists and is {fsize} bytes.')
        continue

    # Splunk earliest/latest query time calulation
    earliest = dt.strftime(splunk_time_format)
    latest = (dt + pd.Timedelta(range_freq) - pd.Timedelta('1ms')).strftime(splunk_time_format)
    print("\net/lt:", dt,ts, key)
    
    # Splunk API call
    try:
        rr = service.jobs.export(query=splunk_query, earliest_time=earliest, latest_time=latest, output_mode="json", sample_ratio=sample_ratio, max_count=max_count)
    except Exception as e:
        print('ERROR ' + str(e))
        continue
    
    # Parse search results
    raw_list = [json.loads(r) for r in rr.read().decode('utf-8').strip().split('\n')]
    search_results = [r['result'] for r in raw_list if r['preview']==False]


    # Store the search results in a pandas data frame (2D size-mutable table)
    df = pd.DataFrame(search_results)
    # "Return a tuple representing the dimensionality of the DataFrame."
    print('results:',df.shape)

    # Initialize empty StringIO object and store dataframe as JSON object in it.
    json_buffer = StringIO()
    df.to_json(json_buffer)

    # Store the StringIO file ojbect in S3.
    print(s3_resource.Object(s3_bucket, key).put(Body=json_buffer.getvalue()))
    

    # Reset our data structures.
    df = None
    raw_list = None
    search_results = None
    json_buffer = None

timer_end = time.time()
timer_duration = timer_end - timer_start

print('\n\nTime Elapsed (Seconds):', timer_duration)
print('\n\n== Done ==')

