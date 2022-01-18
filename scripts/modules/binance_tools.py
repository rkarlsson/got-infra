#!/usr/bin/env python3

import datetime
import requests
import json
from modules.refdata_tools import Symbol, Asset

def get_decimal_places_for_precision(precision):
    count = 0
    while(precision<1):
        precision*=10.0
        count+=1

    return count

class BinanceJSONParse:
    def __init__(self, exchange_url, exchange):
        self.exchange_url = exchange_url
        self.exchange_name = exchange

        # These are the 2 new ones that I'll use standard classes for instead
        self.symbol_map = {}
        self.asset_map = {}

    def get_instruments(self):
        response = requests.get(self.exchange_url)
        for item in json.loads(response.text)["symbols"]:
            if item.get("status", "unknown") != "BREAK" and  item.get("contractStatus", "unknown") != "BREAK":
                exchangePairCode = item["symbol"]
                new_symbol = Symbol()
                new_symbol.set_exchange_name(self.exchange_name)
                new_symbol.set_base_asset(item["baseAsset"].lower())
                new_symbol.set_quote_asset(item["quoteAsset"].lower())
                new_symbol.set_symbol(new_symbol.base_asset.lower()+"-"+new_symbol.quote_asset.lower())
                new_symbol.set_exchange_pair_code(exchangePairCode)

                if(self.exchange_name == "Binance"):
                    new_symbol.set_exchange_id("16")
                elif (self.exchange_name == "BinanceDEX"):
                    new_symbol.set_exchange_id("17")
                elif (self.exchange_name == "BinanceD Futures"):
                    new_symbol.set_exchange_id("18")

                try:
                    contractType = item["contractType"]
                except:
                    contractType = "SPOT"

                try:
                    deliveryDateTmp = datetime.datetime.fromtimestamp(item["deliveryDate"]/1000)
                    deliveryDate = f'{deliveryDateTmp.year:02d}' + "-" + f'{deliveryDateTmp.month:02d}' + "-" + f'{deliveryDateTmp.day:02d}'
                except:
                    deliveryDate = 'null'

                try:
                    new_symbol.set_contract_size(float(item["contractSize"]))
                except:
                    new_symbol.set_contract_size(1.0)

                try:
                    new_symbol.set_maint_margin(float(item["maintMarginPercent"]))
                except:
                    new_symbol.set_maint_margin(0.0)

                try:
                    new_symbol.set_required_margin(float(item["requiredMarginPercent"]))
                except:
                    new_symbol.set_required_margin(0.0)

                if contractType == "PERPETUAL":
                    new_symbol.set_instrument_type("perpetual-future")
                elif contractType.find("QUARTER") != -1:
                    new_symbol.set_instrument_type("future")
                elif contractType == "SPOT":
                    new_symbol.set_instrument_type("spot")
                else:
                    if(deliveryDate=="2100-12-25"):
                        new_symbol.set_instrument_type("perpetual-future")
                    else:
                        new_symbol.set_instrument_type("unknown")

                tickSize = None
                stepSize = None
                for item2 in item["filters"]:
                    if "tickSize" in item2.keys():
                        tickSize = float(item2["tickSize"])
                    elif (item2["filterType"]=="LOT_SIZE") and ("stepSize" in item2.keys()):
                        stepSize = float(item2["stepSize"])

                try:
                    new_symbol.set_price_prec(int(item["pricePrecision"]))
                except:
                    if tickSize is not None:
                        new_symbol.set_price_prec(get_decimal_places_for_precision(tickSize))
                    else:
                        new_symbol.set_price_prec(8)

                try:
                    new_symbol.set_qty_prec(int(item["quantityPrecision"]))
                except:
                    if stepSize is not None:
                        new_symbol.set_qty_prec(get_decimal_places_for_precision(stepSize))
                    else:
                        new_symbol.set_qty_prec(8)
                new_symbol.set_expiry(str(deliveryDate))
                new_symbol.set_tick_size(tickSize)
                new_symbol.set_step_size(stepSize)
                self.symbol_map[exchangePairCode] = new_symbol
    

    def get_assets(self):
        self.asset_map.clear()
        response = requests.get(self.exchange_url)
        for item in json.loads(response.text)["symbols"]:
            if item.get("status", "unknown") != "BREAK" and  item.get("contractStatus", "unknown") != "BREAK":
                code = item["baseAsset"].lower()
                if(code not in self.asset_map):
                    new_asset = Asset()
                    new_asset.set_asset_code(code)
                    new_asset.set_asset_name(code[0:49])
                    new_asset.set_asset_type("cryptocurrency")
                    self.asset_map[code] = new_asset
                code = item["quoteAsset"].lower()
                if(code not in self.asset_map):
                    new_asset = Asset()
                    new_asset.set_asset_code(code)
                    new_asset.set_asset_name(code[0:49])
                    new_asset.set_asset_type("cryptocurrency")
                    self.asset_map[code] = new_asset
