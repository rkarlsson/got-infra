#!/usr/bin/env python3

import boto3
import re
from botocore.exceptions import ClientError
import glob
import os
import subprocess
import logging
import mysql.connector
import zipfile as zf
from datetime import date
from pathlib import Path
from modules.refdata_tools import RefDBAccess

try:
    import zlib
    compression = zf.ZIP_DEFLATED
except:
    compression = zf.ZIP_STORED

modes = { zf.ZIP_DEFLATED: 'deflated',
          zf.ZIP_STORED:   'stored',
          }


############################################################################
# This handles the filenames and upload naming convention etc
############################################################################
class FileNameManager:
    def __init__(self, dirpath):
        self.path = dirpath
        self.date = date.today().strftime("%Y-%m-%d")
        self.good_pairs = []
        self.bad_pairs = []
        refdb = RefDBAccess()
        refdb.get_all_symbols(False)
        self.symbol_map = refdb.symbol_map_id

        self.build_pairs_list()
    
    def get_file_size(self, filename):
        return(Path(filename).stat().st_size)

    def build_pairs_list(self):
        json_files = glob.glob(self.path)
        for json_file in json_files:
            # All files that are older than today and that ends with _all.txt are used to filter with
            filename = json_file.split("/")[-1]
            file_instrument_id = str(filename.split("_")[-2:-1][0])
            self.symbol_details = self.symbol_map[file_instrument_id]
            file_date = filename.split("_")[0]
            file_symbol_name = "_".join(filename.split("_")[1:-2])
            file_dirname = os.path.dirname(json_file)
            snapshot_filename = file_dirname + "/" + file_date + "_" + file_symbol_name + "_" + file_instrument_id + "_ss.txt"
            output_bin_filename = file_dirname + "/" + file_date + "_" + file_symbol_name + "_" + file_instrument_id + ".bin"

            if(json_file.endswith("_all.txt") and self.date not in json_file):
                if(os.path.isfile(snapshot_filename)) and (self.get_file_size(json_file) > 0) and (self.get_file_size(snapshot_filename) > 0):
                    # we need to add this to the good_pairs list
                    bin_upload_path = "binary/capture/" + self.symbol_details.exchange_name + "/" + file_date + "/"
                    json_upload_path = "capture/" + self.symbol_details.exchange_name + "/" + file_date + "/"
                    new_entry = {   
                                    "json_file_all":json_file, 
                                    "json_file_ss":snapshot_filename, 
                                    "output_filename":output_bin_filename,
                                    "bin_upload_path":bin_upload_path,
                                    "json_upload_path":json_upload_path,
                                    "instrument_id":file_instrument_id
                                }
                    self.good_pairs.append(new_entry)

                else:
                    # we need to add this to the bad_pairs list
                    filename = json_file.split("/")[-1]
                    snapshot_filename = file_dirname + "/" + filename.split("_")[:1][0] + "_ss.txt"
                    new_entry = {   "json_file_all":json_file, 
                                    "json_file_ss":snapshot_filename
                                }
                    self.bad_pairs.append(new_entry)

    def generate_binary_from_json(self, json_file, snapshot_filename, output_bin_filename, file_instrument_id):
        symbol_details = self.symbol_map[file_instrument_id]
        logging.info("Generating Binary file: " + output_bin_filename + " from JSON file: " + json_file + " with snapshot file: " + snapshot_filename)
        # Set the file header details
        command_to_execute = ["/GoT/build/bin/convert_md_binance"]
        command_to_execute.append("-i") 
        command_to_execute.append(json_file)
        command_to_execute.append("-I") 
        command_to_execute.append(snapshot_filename)
        command_to_execute.append("-o")
        command_to_execute.append(output_bin_filename)
        command_to_execute.append("-s")
        command_to_execute.append(file_instrument_id)
        command_to_execute.append("-e")
        command_to_execute.append(str(symbol_details.exchange_id))
        command_to_execute.append("-P")
        command_to_execute.append(str(symbol_details.price_precision))
        command_to_execute.append("-Q")
        command_to_execute.append(str(symbol_details.quantity_precision))
        command_to_execute.append("-T")
        command_to_execute.append("{:.10f}".format(symbol_details.tick_size))
        command_to_execute.append("-S")
        command_to_execute.append("{:.10f}".format(symbol_details.step_size))
        command_to_execute.append("-C")
        command_to_execute.append("{:.2f}".format(symbol_details.contract_size))
        logging.info(command_to_execute)
        result = subprocess.check_output(command_to_execute)
        logging.info("Output from command: " + str(result))
        logging.info("Finished generating binary file")
        return(output_bin_filename)


############################################################################
# Simple remove
############################################################################
def removeLocalFile(fileName):
    logging.info("Removing local file: " +fileName)
    if(os.path.isfile((fileName))):
        os.remove(fileName)


############################################################################
# Simple zip of a local file
############################################################################
def zipLocalFile(filename, zip_file_name):
    logging.info("Zipping file: " + filename + " -> " + zip_file_name)
    filename_within_zip = filename.split("/")[-1]
    return_value = False
    with zf.ZipFile(zip_file_name,'w') as zip:
        try:
            zip.write(filename, filename_within_zip, compress_type=compression)
        except:
            logging.info("Errors accounted when trying to zip the file")
        finally:
            if zip.testzip() == None:
                return_value = True
            zip.close()
    return(return_value)

############################################################################
# Uploads a local file to a specified bucket/location
############################################################################
def uploadFileToS3(filename, bucket_name, upload_path):
    S3_filename = upload_path + filename.split('/')[-1]
    logging.info("Uploading local file (" + filename + ") to s3 bucket location: s3://" + bucket_name + "/" + S3_filename)
    s3_cl = boto3.client('s3')
    try:
        # response = s3_cl.upload_file(filename, bucket_name, prefix_path + "/" + filename.split('/')[-1])
        s3_cl.upload_file(filename, bucket_name, S3_filename)
    except ClientError as e:
        logging.info("Error occurred in uploadFileToS3 method: " + e)
        return False
    return True


############################################################################
# Main loop
############################################################################
def main():
    logging.basicConfig(filename='/home/ubuntu/process_captured_data.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

    # Build up the list of files to work with
    files = FileNameManager("/datacollection/binance/*.txt")

    logging.info("Found: " + str(len(files.good_pairs)) + " of good pairs")
    # Iterate over the good pairs
    for entry in files.good_pairs:
        print(entry)
        # Generate the binary file
        bin_file = files.generate_binary_from_json( entry["json_file_all"],
                                                    entry["json_file_ss"],
                                                    entry["output_filename"],
                                                    entry["instrument_id"])

        # Compress json files
        zipLocalFile(entry["json_file_all"], entry["json_file_all"] + ".zip")
        zipLocalFile(entry["json_file_ss"], entry["json_file_ss"] + ".zip")

        # Upload files to S3
        uploadFileToS3(entry["output_filename"], 'got-data', entry["bin_upload_path"])
        uploadFileToS3(entry["json_file_all"] + ".zip", 'got-data', entry["json_upload_path"])
        uploadFileToS3(entry["json_file_ss"] + ".zip", 'got-data', entry["json_upload_path"])        

        # Cleanup the local files
        removeLocalFile(entry["output_filename"])
        removeLocalFile(entry["json_file_all"] + ".zip")
        removeLocalFile(entry["json_file_all"])
        removeLocalFile(entry["json_file_ss"] + ".zip")
        removeLocalFile(entry["json_file_ss"])

    if(len(files.bad_pairs) > 0):
        logging.info("Found: " + str(len(files.bad_pairs)) + " of bad pairs, removing files")    
        # Iterates over the bad pairs
        for entry in files.bad_pairs:
            logging.info(entry)
            # Will delete these files - after logging
            removeLocalFile(entry["json_file_all"])
            removeLocalFile(entry["json_file_ss"])

if __name__ == "__main__":
    main()
