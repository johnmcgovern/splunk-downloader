#!/usr/bin/env python3
# coding: utf-8

#
# config.py - Configuration variables stored here for portability.
# cp config_sample.py config.py to get started.
#

# AWS: Output Configuration
aws_region_name = 'us-west-2'
aws_s3_bucket = 'splunk-export-to-s3'
aws_s3_base_key = 'inbox/bot_signal_raw/' # file path within the S3 bucket

# Splunk Host Configuration
splunk_host = 'es.splk.me'
splunk_port = 8089
splunk_time_format = '%Y-%m-%dT%H:%M:%S.%f'

# Splunk API Token Configuraiton
# If the "raw" token string is provided, use it first. 
# Otherwise pull the token from AWS SSM Parameter Store.
splunk_api_token_raw = ''  # Raw text of the Splunk API token.
splunk_api_token_ssm = 'splunk_api_token'  # AWS System Manager Parameter Name

# Splunk: Query Configuration
splunk_query = 'search index=_internal sourcetype=splunkd | head 1000 | table *'

# Splunk: Time Range Configuration
start_time_str = '2022-11-07 00:00'
start_time_region = 'us/pacific' #'utc'
range_periods = 1  # Number of time periods to generate.
range_freq = '1h'  # Date/time period length for each exported file. #5min #1h #1d
use_sampling = False

# Number of multiprocessing jobs (threads) allowed to run simultaneously.
# Default Splunk per-user concurrency limit is 50.
# Tune this value based on available API and local resources.
max_concurrent_jobs = 1

# Feature flags (True/False)
vip_to_hostname = True  # Uses the API to jump from VIP to a specific search head hostname
write_to_s3 = True  # Should the script write to AWS S3
write_to_local_file = False  # Should the script write to the local filesystem
debug_mode = True  # Should the script output debug messages
