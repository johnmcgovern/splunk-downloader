#!/usr/bin/env python3
# coding: utf-8

## Downloads data from Splunk API and writes to an S3 bucket or local filesystem.


import boto3
import json
import pandas as pd
import os
import sys
import time

import splunklib.client as spclient

from io import StringIO
from joblib import Parallel, delayed
from pathlib import Path

from config import *


# Print initial config variables
if debug_mode:
    print("\nAWS Region:", aws_region_name)
    print("AWS Bucket:", aws_s3_bucket)
    print("AWS Base Key:",aws_s3_base_key, "\n")
    print("Splunk Host:", splunk_host)
    print("Splunk Port:", splunk_port)
    print("Splunk Query:", splunk_query, "\n")
    print("Flag vip_to_hostname:", vip_to_hostname)
    print("Flag write_to_s3:", write_to_s3)
    print("Flag write_to_local_file:", write_to_local_file)
    print("Flag debug_mode:", debug_mode, "\n")


# Sampling Logic:
# For high data volumes: 
#     Shorten the frequency (more files, smaller size).
#     Use sampling (smaller size, incomplete data).
if use_sampling:
    file_name_template = 'bot_signal_raw_<ts>_<freq>_sampled_<ratio>.json'
    sample_ratio=2
if not use_sampling:
    file_name_template = 'bot_signal_raw_<ts>_<freq>.json'
    sample_ratio=1


# AWS S3 API Setup (Boto3)
if write_to_s3:
    aws_s3 = boto3.Session(region_name=aws_region_name)
    s3_resource = aws_s3.resource('s3')
    s3_client = aws_s3.client('s3')


# Splunk API Token Options:
#   If splunk_api_token_raw is NOT blank, use it
#   Otherwise retrieve the token from the AWS SSM parameter store
if splunk_api_token_raw != '':
    splunk_token = splunk_api_token_raw
    if debug_mode:
        print("\nAPI Token: Using raw (plain text) API token")

if splunk_api_token_raw == '':
    aws_ssm = boto3.Session(region_name=aws_region_name)
    ssm = aws_ssm.client('ssm')
    splunk_param = ssm.get_parameter(Name=splunk_api_token_ssm, WithDecryption=True)
    splunk_token = splunk_param['Parameter']['Value']
    if debug_mode:
        print("\nAPI Token: Using API token retrieved from AWS SSM parameter:", splunk_api_token_ssm)


# Start time in both specific TZ and UTC
start_time = pd.Timestamp(start_time_str, tz=start_time_region)
start_time_utc = start_time.astimezone('utc')


# If vip_to_hostname is True
# Set host to the actual name of the search head
if vip_to_hostname:
    try:
        # Pull the search head FQDN from the /services/servers/info API endpoint.
        old_host = splunk_host
        service = spclient.connect(host=splunk_host,port=splunk_port,token=splunk_token)
        splunk_host = service.info()['host']
        if debug_mode:
            print("\nVIP to Host: Changed from", old_host, "to", splunk_host, "\n")

    except Exception as e:
        print('ERROR: Unable to derive search head hostname:', str(e))
        sys.exit(1)

if not vip_to_hostname:
    if debug_mode:
        print("Hostname:", splunk_host, "\n")


# Open Splunk API session
try:
    service = spclient.connect(host=splunk_host,port=splunk_port,token=splunk_token)
except Exception as e: 
    print('ERROR: Unable to connect to Splunk host:', str(e))
    sys.exit(1)
if debug_mode:
    print("Splunk Session: Opened Splunk API session\n")


# Worker function for multi-processing purposes.
def worker(dt):

    # Construct the filename with timestamp, frequency, and sampling ratio.
    ts = dt.strftime('%Y%m%d%H%M')  # timestamp
    filename = file_name_template.replace('<ts>',ts).replace('<freq>',range_freq)
    if use_sampling:
        filename = filename.replace('<ratio>',str(sample_ratio))
    
    # Check if file exists in S3, if yes print message and move on.
    # Note: Currently this script overwrites existing files.
    key = aws_s3_base_key + f'{dt.year}/{dt.month:02d}/{dt.day:02d}/'+filename
    if write_to_s3:
        result = s3_client.list_objects_v2(Bucket=aws_s3_bucket, Prefix=key)
        if 'Contents' in result:
            fsize = result['Contents'][0]['Size']
            if debug_mode:
                print(f'File Exists: {key} exists and is {fsize/1024/1024} megabytes. Skipping.')

    # Splunk earliest/latest query time calulation
    earliest = dt.strftime(splunk_time_format)
    latest = (dt + pd.Timedelta(range_freq) - pd.Timedelta('1ms')).strftime(splunk_time_format)
    if debug_mode:
        print("Time Range:", dt,ts, key, "\n")


    # Splunk API Query Export Call
    try:
        rr = service.jobs.export(
            query=splunk_query, 
            earliest_time=earliest, 
            latest_time=latest, 
            output_mode="json", 
            sample_ratio=sample_ratio, 
            count=0, 
            max_count=123456789,
            timeout=86400
            )
    except Exception as e:
        print('Splunk API Export Error:', str(e))
        sys.exit(1)

    if debug_mode:
        print("API Query: Splunk API call (/search/jobs/export) complete.")
        print
    

    # Parse search results
    try:
        raw_list = [json.loads(r) for r in rr.read().decode('utf-8').strip().split('\n')]
    except Exception as e:
        print('Results Parsing Error:', str(e))
        sys.exit(1)
    if debug_mode:
        print("Results raw_list: Parsed results of Splunk query.")

    # Store the search results in a pandas data frame (2D size-mutable table)
    df = pd.DataFrame([r['result'] for r in raw_list if r['preview']==False])
    if debug_mode:
        # "Return a tuple representing the dimensionality of the DataFrame."
        print('Data Frame Dimensions:',df.shape)

    # Initialize empty StringIO object and store dataframe as JSON object in it.
    json_buffer = StringIO()
    df.to_json(json_buffer)
    if debug_mode:
        print("File Buffer: Wrote dataframe to json buffer for output.")


    # Store the StringIO file ojbect to the local file system.
    if write_to_local_file:
        home_path = os.path.dirname(__file__)
        local_file_path = home_path + "/data/" + aws_s3_bucket + "/" + key
        output_file = Path(local_file_path)

        if debug_mode:
            print("Local File: Writing file to:", local_file_path)

        output_file.parent.mkdir(exist_ok=True, parents=True)
        output_file.write_text(json_buffer.getvalue())

        if debug_mode:
            print("Local File: Write completed. Size:", round(os.path.getsize(local_file_path)/1024/1024,2), "MB")
    

    # Store the StringIO file ojbect in S3.
    if write_to_s3: 
        if debug_mode:
            print("AWS S3:", s3_resource.Object(aws_s3_bucket, key).put(Body=json_buffer.getvalue()))
        if not debug_mode:
            s3_resource.Object(aws_s3_bucket, key).put(Body=json_buffer.getvalue())

        if debug_mode:
            print("Wrote to S3:", "Bucket-", aws_s3_bucket, "Key-", key)

    print("Job Complete:", dt, df.shape, "\n")


# 
# Main Multiprocessing Loop with Timer
#
timer_start = time.time()

result = Parallel(n_jobs=max_concurrent_jobs, prefer="threads")(delayed(worker)(dt) for dt in pd.date_range(start=start_time_utc, periods=range_periods, freq=range_freq))

timer_end = time.time()

print('\nTime Elapsed:', round(timer_end - timer_start, 2), "seconds")
print('\n== Done ==')

