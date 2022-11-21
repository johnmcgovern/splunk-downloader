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


# Wrapper function for console logging
def l2c(*args):
    if log_to_console:
        print(*args)


# Setup logging file
if log_to_file:
    home_path = os.path.dirname(os.path.abspath(__file__))
    log_file_path = home_path + "/logs/Splunk_Downloader_" + str(time.time()) + ".log"
    file_writer = open(log_file_path, "a")

# Wrapper function for local file logging
def l2f(*args):
    if log_to_file:
        log_file_text = str(time.time()) + " "
        for arg in args:
            log_file_text += " ".join(str(arg).split()) + "\n" 
        file_writer.write(log_file_text)


# Print initial config variables
l2c("\nAWS Region:", aws_region_name)
l2c("AWS Bucket:", aws_s3_bucket)
l2c("AWS Base Key:",aws_s3_base_key, "\n")
l2c("Splunk Host:", splunk_host)
l2c("Splunk Port:", splunk_port)
l2c("Splunk Query:", splunk_query, "\n")
l2c("Flag vip_to_hostname:", vip_to_hostname)
l2c("Flag write_to_s3:", write_to_s3)
l2c("Flag write_to_local_file:", write_to_local_file)
l2c("Flag log_to_console:", log_to_console)
l2c("Flag log_to_file:", log_to_file)

l2f(f'message="Starting Splunk_Downloader.py"')
l2f(f'message="Initial Parameters" \
    aws_region_name="{aws_region_name}" \
    aws_s3_bucket="{aws_s3_bucket}" \
    aws_s3_base_key="{aws_s3_base_key}" \
    splunk_host="{splunk_host}" \
    splunk_port="{splunk_port}" \
    splunk_query="{splunk_query}" \
    vip_to_hostname="{vip_to_hostname}" \
    write_to_s3="{write_to_s3}" \
    write_to_local_file="{write_to_local_file}" \
    log_to_console="{log_to_console}" \
    log_to_file="{log_to_file}"')


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
    l2c("API Token: Using raw (plain text) API token")
    l2f(f'message="Using raw (plain text) API token"')

if splunk_api_token_raw == '':
    aws_ssm = boto3.Session(region_name=aws_region_name)
    ssm = aws_ssm.client('ssm')
    splunk_param = ssm.get_parameter(Name=splunk_api_token_ssm, WithDecryption=True)
    splunk_token = splunk_param['Parameter']['Value']
    l2c("\nAPI Token: Using API token retrieved from AWS SSM parameter:", splunk_api_token_ssm)
    l2f(f'message="Using API token retrieved from AWS SSM parameter" ssm_param="{splunk_api_token_ssm}"')


# Start time in both specific TZ and UTC
start_time = pd.Timestamp(start_time_str, tz=start_time_region)
start_time_utc = start_time.astimezone('utc')

l2c("\nTime start_time:", start_time)
l2c("Time start_time_utc:", start_time_utc)
l2c("Time range_periods:", range_periods)
l2c("Time range_freq:", range_freq)
l2f(f'message=Time Parameters" \
    start_time="{start_time}" \
    start_time_utc="{start_time_utc}" \
    range_periods="{range_periods}" \
    range_freq="{range_freq}"')

# If vip_to_hostname is True
# Set host to the actual name of the search head
if vip_to_hostname:
    try:
        # Pull the search head FQDN from the /services/servers/info API endpoint.
        old_host = splunk_host
        service = spclient.connect(host=splunk_host,port=splunk_port,token=splunk_token)
        splunk_host = service.info()['host']
        l2c("\nVIP to Host: Changed from", old_host, "to", splunk_host, "\n")
        l2f(f'message="VIP to Host Mapping Complete" old_host="{old_host}" host="{splunk_host}"')

    except Exception as e:
        print('ERROR: Unable to derive search head hostname:', str(e))
        sys.exit(1)

if not vip_to_hostname:
    l2c("Hostname:", splunk_host, "\n")
    l2f(f'message="Hostname left as original" old_host="" host="{splunk_host}"')


# Open Splunk API session
try:
    service = spclient.connect(host=splunk_host,port=splunk_port,token=splunk_token)
except Exception as e: 
    print('ERROR: Unable to connect to Splunk host:', str(e))
    sys.exit(1)
l2c("Splunk Session: Opened Splunk API session\n")
l2f(f'message="Opened Splunk API session"')



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
            l2c(f'File Exists: {key} exists and is {fsize/1024/1024} megabytes. Skipping.')

    # Splunk earliest/latest query time calulation
    # date/time strftime format:
    # earliest = dt.strftime(splunk_time_format)
    # latest = (dt + pd.Timedelta(range_freq) - pd.Timedelta('1ns')).strftime(splunk_time_format)
    # epoch:
    earliest = dt.timestamp()
    latest = (dt + pd.Timedelta(range_freq) - pd.Timedelta('1ns')).timestamp()
    l2c("---")
    l2c("Time Zone:", start_time_region)
    l2c("Time Earliest:", earliest)
    l2c("Time Latest:", latest)
    l2c("Time Range:", dt,ts, key)
    l2f(f'message="Time Parameters" start_time_region="{start_time_region}" et="{earliest}" \
        lt="{latest}" range_periods="{range_periods}" range_freq="{range_freq}"')


    # Splunk API Query Export Call
    l2c("API Query: Initial Splunk API call (/search/jobs/export) started.")
    l2f(f'message="API Query: Initial Splunk API call (/search/jobs/export) started"')
    try:
        api_timer_start = time.time()
        rr = service.jobs.export(
            query=splunk_query, 
            earliest_time=earliest, 
            latest_time=latest, 
            output_mode="json", 
            sample_ratio=sample_ratio, 
            count=0, 
            max_count=123456789,
            timeout=86400,
            adhoc_search_level="smart",
            )
        api_timer_end = time.time()
    except Exception as e:
        print('Splunk API Export Error:', str(e))
        sys.exit(1)
    api_timer = round(api_timer_end - api_timer_start, 2)
    l2c("API Query: Initial Splunk API call (/search/jobs/export) complete. Timer:", api_timer, "seconds")
    l2f(f'message="API Query: Initial Splunk API call (/search/jobs/export) complete" api_timer="{api_timer}"')

    # Read & Parse Search Results from Splunk API
    try:
        parse_timer_start = time.time()
        l2c("API Query: Starting results download.")
        l2f(f'"API Query: Starting results download."')
        raw_list = [json.loads(r) for r in rr.read().decode('utf-8').strip().split('\n')]
        parse_timer_end = time.time()
    except Exception as e:
        print('Results Parsing Error:', str(e))
        sys.exit(1)
    parse_timer = round(parse_timer_end - parse_timer_start, 2)
    l2c("API Query: Downloaded and parsed query results. raw_list_len:", len(raw_list), "Timer:", parse_timer, "seconds")
    l2f(f'message="API Query: Downloaded and parsed query results." raw_list_len="{len(raw_list)}" parser_timer="{parse_timer}"')

    # Exit the script if there are no results returned
    if len(raw_list) <= 1:
        print("API Query: Query returned no results. Exiting.")
        l2f(f'message="API Query: Query returned no results. Exiting"')
        sys.exit(1)

    # Store the search results in a pandas data frame (2D size-mutable table)
    pandas_timer_start = time.time()
    df = pd.DataFrame([r['result'] for r in raw_list if r['preview']==False])
    # "Return a tuple representing the dimensionality of the DataFrame."
    l2c('DataFrame Dimensions:',df.shape)
    l2f(f'messgae="DataFrame Dimensions" df_shape="{df.shape}"')

    # Initialize empty StringIO object and store dataframe as JSON object in it.
    json_buffer = StringIO()
    df.to_json(json_buffer)
    pandas_timer_end = time.time()
    pandas_timer = round(pandas_timer_end - pandas_timer_start, 2)
    l2c("DF -> Buffer: Wrote DataFrame to JSON buffer for output.", pandas_timer, "seconds")
    l2f(f'message="DF -> Buffer: Wrote DataFrame to JSON buffer for output" pandas_timer="{pandas_timer}"')

    # Store the StringIO file ojbect to the local file system.
    if write_to_local_file:
        # home_path = os.path.dirname(__file__)
        home_path = os.path.dirname(os.path.abspath(__file__))
        local_file_path = home_path + "/data/" + aws_s3_bucket + "/" + key
        output_file = Path(local_file_path)

        l2c("Local File: Writing file to:", local_file_path)
        l2f(f'message="Local File: Writing file to disk" local_file_path="{local_file_path}"')

        output_file.parent.mkdir(exist_ok=True, parents=True)
        output_file.write_text(json_buffer.getvalue())

        local_file_size = round(os.path.getsize(local_file_path)/1024/1024,2)
        l2c("Local File: Write completed. Size:", local_file_size, "MB")
        l2f(f'message="Local File: Write completed." local_file_size="{local_file_size}"')


    # Store the StringIO file ojbect in S3.
    if write_to_s3: 
        l2c("AWS S3:", s3_resource.Object(aws_s3_bucket, key).put(Body=json_buffer.getvalue()))
        if not log_to_console:
            s3_resource.Object(aws_s3_bucket, key).put(Body=json_buffer.getvalue())

        l2c("Wrote to S3:", "Bucket-", aws_s3_bucket, "Key-", key)
        l2f(f'message="Wrote to S3" aws_s3_bucket="{aws_s3_bucket}" aws_s3_key="{key}"')

    print("Job Complete:", dt, df.shape)
    l2c("---\n")
    l2f(f'message="Job Completed" dt="{dt}" df_shape="{df.shape}"')



# 
# Main Multiprocessing Loop with Timer
#
timer_start = time.time()
result = Parallel(n_jobs=max_concurrent_jobs, prefer="threads")(delayed(worker)(dt) for dt in pd.date_range(start=start_time_utc, periods=range_periods, freq=range_freq))
timer_end = time.time()

total_runtime = round(timer_end - timer_start, 2)
l2c('\nTotal Runtime:', total_runtime, "seconds")
l2c('\n== Done ==')
l2f(f'message=Stopped" total_runtime="{total_runtime}"')

