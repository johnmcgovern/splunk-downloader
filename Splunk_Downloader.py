#!/usr/bin/env python
# coding: utf-8

## Downloads data from Splunk and writes to an S3 bucket


import boto3
import json
import pandas as pd
import pytz

from io import StringIO

import splunklib.client as spclient
import splunklib.results as results


# AWS: Output Configuration
s3_bucket = 'splunk-export-to-s3'
s3_base_key = 'inbox/bot_signal_raw/'


# Splunk: API Configuration
splunk_api_token_name = 'splunk_api_token'
HOST = 'es.splk.me'
PORT = 8089
splunk_time_format = '%Y-%m-%dT%H:%M:%S.%f'


# Splunk: Time Range Configuration
start_time_str = '2022-10-21 00:00'
start_time_region = 'us/pacific' #'utc'
range_periods = 54
range_freq = '1min'  # Data time period for each exported file.
use_sampling = False


# Splunk: Query Configuration
# splunk_query = 'search index=summary_cisbot sourcetype=stash signal=*'
splunk_query = 'search index=_internal sourcetype=splunkd | head 10'

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
aws = boto3.Session(region_name='us-west-2')
ssm = aws.client('ssm')
#service = spclient.connect(host=HOST,port=PORT,token=splunk_token)
s3_resource = aws.resource('s3')
s3_client = aws.client('s3')


# Splunk SDK Session setup w/ SSM Params
splunk_param = ssm.get_parameter(Name=splunk_api_token_name, WithDecryption=True)
splunk_token = splunk_param['Parameter']['Value']

start_time = pd.Timestamp(start_time_str, tz=start_time_region)
start_time_utc = start_time.astimezone('utc')


# 
# Main data export loop
#
for dt in pd.date_range(start=start_time_utc, periods=range_periods, freq=range_freq):

    service = spclient.connect(host=HOST,port=PORT,token=splunk_token)
    ts = dt.strftime('%Y%m%d%H%M')
    
    filename = file_name_template.replace('<ts>',ts).replace('<freq>',range_freq)
    if use_sampling:
        filename = filename.replace('<ratio>',str(sample_ratio))
    
    # Check if file exists in S3
    key = s3_base_key + f'{dt.year}/{dt.month:02d}/{dt.day:02d}/'+filename
    result = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=key)
    if 'Contents' in result:
        fsize = result['Contents'][0]['Size']
        print(f'{key} exists and is {fsize} bytes.')
        continue

    # Splunk query time 
    earliest = dt.strftime(splunk_time_format)
    latest = (dt + pd.Timedelta(range_freq) - pd.Timedelta('1ms')).strftime(splunk_time_format)
    print(dt,ts, key)
    
    # Splunk API call
    try:
        rr = service.jobs.export(query=splunk_query, earliest_time=earliest, latest_time=latest, output_mode="json", sample_ratio=sample_ratio)
    except Exception as e:
        print('ERROR ' + str(e))
        continue
    
    # Parse search results
    raw_list = [json.loads(r) for r in rr.read().decode('utf-8').strip().split('\n')]
    print("Raw Results:", raw_list)
    search_results = [r['result'] for r in raw_list if r['preview']==False]


    df = pd.DataFrame(search_results)
    print('results:',df.shape)

    json_buffer = StringIO()
    df.to_json(json_buffer)
    print(s3_resource.Object(s3_bucket, key).put(Body=json_buffer.getvalue()))
    
    # Reset
    df = None
    raw_list = None
    json_buffer = None

print('...done')






