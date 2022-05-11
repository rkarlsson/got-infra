import time
from web3 import Web3
import json
from tmp_abi import *
from writer import *

bsc="https://bsc-dataseed.binance.org/"
web3 = Web3(Web3.HTTPProvider(bsc))

# uniswap_Factory
f = open("../IUniswapV2Factory.json")
factory_abi = json.load(f)["abi"]
factory_address = '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73'
factory_contract = web3.eth.contract(address=factory_address, abi=factory_abi)

allPairsLength = factory_contract.functions.allPairsLength().call()

f = open("../IUniswapV2Pair.json")
pairs_abi = json.load(f)["abi"]

w = Writer()

while(1):
    allPairs_address = "0x28415ff2C35b65B9E5c7de82126b4015ab9d031F"
    contract = web3.eth.contract(address=allPairs_address, abi=pairs_abi)
    symbol = contract.functions.name().call()
    supply = contract.functions.totalSupply().call()
    t0     = contract.functions.token0().call()
    t1     = contract.functions.token1().call()
    reserves = contract.functions.getReserves().call()
    contract0 = web3.eth.contract(address=t0, abi=tmp_abi)
    contract1 = web3.eth.contract(address=t1, abi=tmp_abi)
    tob = t_tob_state.alloc(0, reserves[0]/float(reserves[1]))
    w.send_tob(tob)

    print("%s Price for %s/%s = %f" % (allPairs_address, contract1.functions.name().call(), contract0.functions.name().call(), reserves[0]/float(reserves[1])))
    time.sleep(1)
