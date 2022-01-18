#!/usr/bin/python3

from datetime import date
import os.path as path
import time
import glob
import boto3
from botocore.exceptions import ClientError
import argparse
import os


def exist_in_s3(client, bucket, filename, exchange_name):
    # Returns True if file exist, otherwise False
    S3_Static_Prefix_path = "binary/tardis/" + exchange_name + "/"
    file_only_name = filename.split('/')[-1]
    date_on_file = file_only_name.split('_')[0]
    s3_full_path = S3_Static_Prefix_path + date_on_file + "/" + file_only_name

    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=s3_full_path,
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == s3_full_path:
            return(True)
    return(False)

def is_old_enough(filename):
    mod_time = path.getmtime(filename)
    curr_time = time.time()
    timediff = (curr_time - mod_time)
    if(timediff > 30):
        return(True)
    else:
        return(False)

def uploadFileToS3(client, filename, bucket_name, exchange_name):
    S3_Static_Prefix_path = "binary/tardis/" + exchange_name + "/"
    file_only_name = filename.split('/')[-1]
    date_on_file = file_only_name.split('_')[0]
    upload_file_name = S3_Static_Prefix_path + date_on_file + "/" + file_only_name
    print("Uploading: " + filename + " to: " + bucket_name + "/" + upload_file_name)

    try:
        client.upload_file(filename, bucket_name, upload_file_name)
    except ClientError as e:
        return False
    return True

def removeLocalFile(fileName):
    os.remove(fileName)

def main():

    parser = argparse.ArgumentParser(description="Downloads csv files and converts/merge to a standard binary format")
    parser.add_argument('--exchange', dest='exchange', default="Binance")
    args = parser.parse_args()

    current_exchange = {    "Binance": "binance", 
                            "Binance Futures":"binance-futures",
                            "BinanceDEX":"binance-delivery",
                            "FTX":"ftx"}
    
    refdb_exchange_name = args.exchange
    tardis_exchange_name = current_exchange[refdb_exchange_name]    

    session = boto3.session.Session()
    s3 = session.client('s3')

    # root_dir needs a trailing slash (i.e. /root/dir/)
    for filename in glob.iglob("/data/tardis/" + refdb_exchange_name + "/" + '**/*.bin', recursive=True):
        if(is_old_enough(filename)):
            if(not exist_in_s3(s3, 'got-data', filename, refdb_exchange_name)):
                uploadFileToS3(s3, filename, 'got-data', refdb_exchange_name)
                print("Deleting local file: " + filename + " now uploaded")
                removeLocalFile(filename)                
            else:
                print("Deleting local file: " + filename + " as it already exists in S3")
                removeLocalFile(filename)
            

if __name__ == "__main__":
    main()