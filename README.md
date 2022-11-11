# Splunk_Downloader
 
This python script is used to download data from the Splunk API and write it to S3 or the local filesystem.

## Setup

- cp config_samply.py to config.py
- Modify config.py variables to suite the specific environment
- Run the script: ./Splunk_Downloader.py

## Flags (True/False)

- vip_to_hostname: When true the script uses an API call to the VIP to derive the hostname of the individual search head. Used to work around timeout issues.
- write_to_s3: When true, the script writes to an S3 bucket.
- write_to_local_file: When true, the script writes to the local file system.
- debug_mode: Enables verbose logging for testing.

## Notes

- The "single" and "multi" versions have been deprecated in favor of the unified "Splunk_Downloader.py". 
- A seperate config file was added to support portability and ease of testing.
