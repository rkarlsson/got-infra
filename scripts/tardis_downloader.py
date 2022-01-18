#!/usr/bin/env python3

from tardis_dev import datasets, get_exchange_details
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as np
import traceback
import boto3
from botocore.exceptions import ClientError
import subprocess
import argparse
import logging
import os
from modules.refdata_tools import Symbol, RefDBAccess
from os.path import exists

api_key = "TD.RqK4Y7OSvpsvAleb.Xx7iuf-XX9V86DO.8Z0PfckIAyaRTL1.4-qwXpUEwMwzGrZ.zy3SGfRocFeXVgT.Nfgv"
local_base_path = "/home/rkarlsson/data/tardis/"


# new class to work on finding objects in s3 faster..
class s3_objects:
    def __init__(self, bucket_name):
        self.s3 = boto3.resource('s3')
        print("Getting list of files from bucket")
        self.bucket = self.s3.Bucket(bucket_name)
        self.objects = self.bucket.objects.all()
        self.file_list = []
        self.pair_code_dict = {}
        counter = 0
        for object in self.objects:
            if(object.key.endswith(".bin")):
                end_filename = object.key.split('/')[-1]
                pair_code = end_filename.split('_')[1]
                self.file_list.append(end_filename)
                if (pair_code not in self.pair_code_dict):
                    self.pair_code_dict[pair_code] = []
                self.pair_code_dict[pair_code].append(end_filename)
                counter += 1
                if(counter > 1000):
                    print(".", end='')
                    counter = 0
        print("\nFinished getting list of files from bucket")

    def exist_in_s3(self, filename):
        end_filename = filename.split('/')[-1]
        pair_code = end_filename.split('_')[1]
        if (pair_code in self.pair_code_dict):
            for file in self.pair_code_dict[pair_code]:
                if(file == end_filename):
                    return(True)
        return(False)

def altered_file_name(exchange, data_type, date, symbol, format):
    new_symname = symbol.replace("/","-")
    # print(new_symname)
    return f"{exchange}_{data_type}_{date.strftime('%Y-%m-%d')}_{new_symname}.{format}.gz"

def download_symbol(tardis_exchange, symbol, from_date, to_date):
    download_types = ["incremental_book_L2", "trades", "quotes"]
    datasets.download(
        exchange=tardis_exchange,
        data_types=download_types,
        from_date=from_date,
        to_date=to_date,
        symbols=[symbol],
        download_dir=local_base_path,
        get_filename=altered_file_name,
        api_key=api_key
    )

def generate_binary(parameters):
    my_env = {**os.environ, 'USER': 'rkarlsson'}
    os.spawnvpe(os.P_NOWAIT, "./generate_binary.py", parameters, my_env)

def exist_in_s3(client, bucket, filename, exchange_name):
    # Returns True if file exist, otherwise False
    S3_Static_Prefix_path = "binary/tardis/" + exchange_name + "/"
    file_only_name = filename.split('/')[-1]
    date_on_file = file_only_name.split('_')[0]
    s3_full_path = S3_Static_Prefix_path + date_on_file + "/" + file_only_name

    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=s3_full_path
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == s3_full_path:
            return(True)
    return(False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Downloads csv files and converts/merge to a standard binary format")
    parser.add_argument('--exchange', dest='exchange', default="Binance")

    args = parser.parse_args()

    # set statically for now
    current_exchange = {    "Binance": "binance", 
                            "Binance Futures":"binance-futures",
                            "BinanceDEX":"binance-delivery",
                            "FTX":"ftx"}
    
    refdb_exchange_name = args.exchange
    tardis_exchange_name = current_exchange[refdb_exchange_name]

    # First - download all internal refdata..
    print("Loading reference data from database")
    refdb = RefDBAccess()
    refdb.get_all_symbols(False)
    symbol_map = refdb.symbol_map_id
    symbol_list = symbol_map.values()
    print("Fetched: " + str(len(symbol_list)) + " symbols")

    # This class accelerates all future object lookups. Doing it individually takes hours when many objects in bucket
    s3_details = s3_objects('got-data')

    # Get all exchangespecific details from tardis
    exchange_details = get_exchange_details(tardis_exchange_name)
    num_symbols = len(exchange_details["availableSymbols"])

    session = boto3.session.Session()
    s3 = session.client('s3')

    # Iterate through all symbols that Tardis has for this exchange
    for tardis_sym in exchange_details["availableSymbols"]:
        pair_code = tardis_sym["id"].upper() # uppercase for paircode
        for sym in symbol_list:
            if(sym.exchange_pair_code == pair_code):
                if(sym.exchange_name == refdb_exchange_name): # this means we have a match in the internal reference data
                    # now get all the available dates for this particular symbol
                    print("Checking binary files for: " + pair_code + " (InstrumentId = " + str(sym.instrument_id) + ")")
                    from_date = "2021-07-17"
                    generic_to_date = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d') + "T00:00:00.000Z"
                    availableSince = max([pd.to_datetime(tardis_sym["availableSince"]),pd.to_datetime(from_date).tz_localize("UTC")])
                    availableTo    = pd.to_datetime(tardis_sym.get("availableTo", generic_to_date))
                    to_date = min([pd.to_datetime(datetime.now() - timedelta(hours=4)).tz_localize('UTC'), availableTo]).strftime("%Y-%m-%d")
                    dates = [d.strftime("%Y-%m-%dT%H:%M:%S.00Z") for d in pd.date_range(start=availableSince, end=availableTo).tolist()]

                    if len(dates) > 0:
                        for i in np.arange(0, len(dates)-1):
                            date = dates[i]
                            date1 = dates[i+1]

                            # This will be the final generated output file
                            
                            output_filename = date[:10] + "_" + pair_code.replace("/","-") + "_" + str(sym.instrument_id) + ".bin"
                            local_output_path = local_base_path + refdb_exchange_name + "/" + date[:10]
                            total_out_filename = local_output_path + "/" + output_filename

                            if(not exists(total_out_filename) and not s3_details.exist_in_s3(total_out_filename)):
                                if(not exist_in_s3(s3, 'got-data', total_out_filename, refdb_exchange_name)): # doing this spot check as the static list has not been updated
                                    print("Downloading data for pairCode: " + pair_code + " - Date: " + date[:10])
                                    download_symbol(tardis_exchange_name, pair_code, date, date1)
                                    parameters = ["./generate_binary.py", date[:10], pair_code]
                                    parameters.append(str(sym.instrument_id))
                                    parameters.append(str(sym.exchange_id))
                                    parameters.append(str(sym.price_precision))
                                    parameters.append(str(sym.quantity_precision))
                                    parameters.append(str(sym.tick_size))
                                    parameters.append(str(sym.step_size))
                                    parameters.append(str(sym.contract_size))
                                    parameters.append(tardis_exchange_name)
                                    parameters.append(refdb_exchange_name)
                                    generate_binary(parameters)
