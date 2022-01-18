#!/usr/bin/python3

# pip install tardis-dev
# requires Python >=3.6
import subprocess
import argparse
import logging
import os
import sys, getopt
from modules.refdata_tools import Symbol
from pathlib import Path


# comment out to disable debug logs
logging.basicConfig(level=logging.ERROR)

# comment out to disable debug logs

def remove_local_file(fileName):
    os.remove(fileName)

def generate_binfile_from_csv(input_file, data_date, data_type, symbol_details):
   
    if(data_type == "incremental_book_L2"):
        conversion_type = "p"
    elif(data_type == "trades"):
        conversion_type = "T"
    elif(data_type == "quotes"):
        conversion_type = "t"
    else:
        return

    # <YYYY-MM-DD_SYMBOL_SYMBOLID.bin/csv>
    output_filename = data_date + "_" + symbol_details.exchange_pair_code.replace("/","-") + "_" + data_type + "_" + str(symbol_details.instrument_id) + ".bin"
    if(os.path.isfile(output_filename)):
        remove_local_file(output_filename)
    # Set the file header details
    command_to_execute = ["/GoT/build/bin/convert_md_tardis"] 
    command_to_execute.append("-i") 
    command_to_execute.append(input_file) 
    command_to_execute.append("-o") 
    command_to_execute.append("/data/tardis/" + output_filename) 
    command_to_execute.append("-t")
    command_to_execute.append(conversion_type) 
    command_to_execute.append("-s") 
    command_to_execute.append(str(symbol_details.instrument_id))
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
    result = subprocess.check_output(command_to_execute)
    remove_local_file(input_file)
    return(output_filename)

def merge_and_upload(data_date, symbol_details):

    # <YYYY-MM-DD_SYMBOL_SYMBOLID.bin/csv>
    output_filename = data_date + "_" + symbol_details.exchange_pair_code.replace("/","-") + "_" + str(symbol_details.instrument_id) + ".bin"
    in_l2_fname = "/data/tardis/" + data_date + "_" + symbol_details.exchange_pair_code.replace("/","-") + "_incremental_book_L2_" + str(symbol_details.instrument_id) + ".bin"
    in_trades_fname = "/data/tardis/" + data_date + "_" + symbol_details.exchange_pair_code.replace("/","-") + "_trades_" + str(symbol_details.instrument_id) + ".bin"
    in_quotes_fname = "/data/tardis/" + data_date + "_" + symbol_details.exchange_pair_code.replace("/","-") + "_quotes_" + str(symbol_details.instrument_id) + ".bin"

    local_output_path = "/data/tardis/" + symbol_details.exchange_name + "/" + data_date
    Path(local_output_path).mkdir(parents=True, exist_ok=True)
    total_out_filename = local_output_path + "/" + output_filename

    if(os.path.isfile(total_out_filename)):
        remove_local_file(total_out_filename)

    # Set the file header details
    command_to_execute = ["/GoT/build/bin/binfile_merger"] 
    command_to_execute.append("-b") 
    command_to_execute.append(in_l2_fname) 
    command_to_execute.append("-b") 
    command_to_execute.append(in_trades_fname) 
    command_to_execute.append("-b")
    command_to_execute.append(in_quotes_fname) 
    command_to_execute.append("-f") 
    command_to_execute.append(total_out_filename)
    result = subprocess.check_output(command_to_execute)
    remove_local_file(in_l2_fname)
    remove_local_file(in_trades_fname)
    remove_local_file(in_quotes_fname)
    print("Built: " + total_out_filename)
    return(output_filename)


def main(argv):
    inputdate = argv[0]
    pair_code = argv[1]

    sym = Symbol()
    sym.set_instrument_id(str(int(argv[2])))
    sym.set_exchange_id(str(int(argv[3])))    
    sym.set_price_prec(float(argv[4]))
    sym.set_qty_prec(float(argv[5]))
    sym.set_tick_size(float(argv[6]))
    sym.set_step_size(float(argv[7]))
    sym.set_contract_size(float(argv[8]))
    sym.set_exchange_name(argv[10])
    sym.set_exchange_pair_code(pair_code)                                

    tardis_exchange_name = argv[9]

    in_path = "/data/tardis/" + tardis_exchange_name
    new_pair_code = pair_code.replace("/","-")
    generate_binfile_from_csv(in_path + "_incremental_book_L2_" + inputdate + "_" + new_pair_code + ".csv.gz", inputdate,"incremental_book_L2", sym)
    generate_binfile_from_csv(in_path + "_trades_" + inputdate + "_" + new_pair_code + ".csv.gz", inputdate,"trades", sym)
    generate_binfile_from_csv(in_path + "_quotes_" + inputdate + "_" + new_pair_code + ".csv.gz", inputdate,"quotes", sym)
    merge_and_upload(inputdate, sym)
    sys.exit()


if __name__ == '__main__':

    main(sys.argv[1:])
