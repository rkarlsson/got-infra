#!/usr/bin/env python3

from web3.auto import w3
from web3 import Web3
import os
import time
import json
from hexbytes import HexBytes
from web3.contract import Contract
from web3._utils.events import get_event_data
from web3._utils.abi import exclude_indexed_event_inputs, get_abi_input_names, get_indexed_event_inputs, normalize_event_input_types
from web3.exceptions import MismatchedABI, LogTopicError
from web3.types import ABIEvent
from eth_utils import event_abi_to_log_topic, to_hex
from hexbytes import HexBytes
from functools import lru_cache

import json
import re


def decode_tuple(t, target_field):
  output = dict()
  for i in range(len(t)):
    if isinstance(t[i], (bytes, bytearray)):
      output[target_field[i]['name']] = to_hex(t[i])
    elif isinstance(t[i], (tuple)):
      output[target_field[i]['name']] = decode_tuple(t[i], target_field[i]['components'])
    else:
      output[target_field[i]['name']] = t[i]
  return output

def decode_list_tuple(l, target_field):
  output = l
  for i in range(len(l)):
    output[i] = decode_tuple(l[i], target_field)
  return output

def decode_list(l):
  output = l
  for i in range(len(l)):
    if isinstance(l[i], (bytes, bytearray)):
      output[i] = to_hex(l[i])
    else:
      output[i] = l[i]
  return output

def convert_to_hex(arg, target_schema):
  """
  utility function to convert byte codes into human readable and json serializable data structures
  """
  output = dict()
  for k in arg:
    if isinstance(arg[k], (bytes, bytearray)):
      output[k] = to_hex(arg[k])
    elif isinstance(arg[k], (list)) and len(arg[k]) > 0:
      target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
      if target['type'] == 'tuple[]':
        target_field = target['components']
        output[k] = decode_list_tuple(arg[k], target_field)
      else:
        output[k] = decode_list(arg[k])
    elif isinstance(arg[k], (tuple)):
      target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
      output[k] = decode_tuple(arg[k], target_field)
    else:
      output[k] = arg[k]
  return output

@lru_cache(maxsize=None)
def _get_contract(address, abi):
  """
  This helps speed up execution of decoding across a large dataset by caching the contract object
  It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
  """
  if isinstance(abi, (str)):
    abi = json.loads(abi)

  contract = w3.eth.contract(address=Web3.toChecksumAddress(address), abi=abi)
  return (contract, abi)

def decode_tx(address, input_data, abi):
  if abi is not None:
    try:
      (contract, abi) = _get_contract(address, abi)
      func_obj, func_params = contract.decode_function_input(input_data)
      target_schema = [a['inputs'] for a in abi if 'name' in a and a['name'] == func_obj.fn_name][0]
      decoded_func_params = convert_to_hex(func_params, target_schema)
      return (func_obj.fn_name, json.dumps(decoded_func_params), json.dumps(target_schema))
    except:
      e = sys.exc_info()[0]
      return ('decode error', repr(e), None)
  else:
    return ('no matching abi', None, None)

def handle_event(event, web3):
  print(event)
    # block = web3.eth.get_block(blockid.hex())
    # print("Found a new block, nr: " + str(block['number']))
    # print("\nTransactions:")
    # for transaction in block['transactions']:
        # tx = web3.eth.get_transaction(transaction.hex())
        # print("From: " + tx['from'] + " :: To: " + tx['to'] + " :: TxIndex: " + str(tx['transactionIndex']) + " :: GasFee: " + str(tx['gas']))
        # output = decode_tx(tx['hash'], tx['input'], sample_abi)
        # print(output)
        # if tx['to'] == '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45':
        #   print(tx)
        #   print("")
    #print(block)

def log_loop(poll_interval, web3):
    # uniswap_Factory
    f = open("IUniswapV2Factory.json")
    factory_abi = json.load(f)["abi"]
    factory_address = '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73'
    factory_contract = web3.eth.contract(address=factory_address, abi=factory_abi)

    allPairsLength = factory_contract.functions.allPairsLength().call()
    f = open("IUniswapV2Pair.json")
    pairs_abi = json.load(f)["abi"]
    block_filter = web3.eth.filter({'fromBlock':'latest'})

    while True:
        for event in block_filter.get_new_entries():
            handle_event(event, web3)
        time.sleep(poll_interval)

def main():
    # PancakeABI = open('pancakeABI','r').read().replace('\n','')
    
    bsc="https://bsc-dataseed.binance.org/"
    web3 = Web3(Web3.HTTPProvider(bsc))
    # web3 = Web3(Web3.IPCProvider('/data/bsc/node/geth.ipc'))
    # ipc_path=os.path.dirname(os.path.realpath(__file__))+'/geth-data/geth.ipc'
    # web3 = Web3(Web3.IPCProvider(ipc_path))
    # print(web3.isConnected())
    
    log_loop(1, web3)

if __name__ == '__main__':
    main()
