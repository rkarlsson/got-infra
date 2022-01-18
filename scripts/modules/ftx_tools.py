#!/usr/bin/env python3

import requests
import json
import datetime
from modules.refdata_tools import Symbol, Asset

def get_decimal_places_for_precision(precision):
    count = 0
    while(precision<1):
        precision*=10.0
        count+=1

    return count

class FTXJSONParse:
    def __init__(self, spot_url, futures_url, asset_url):
        self.spot_url = spot_url
        self.futures_url = futures_url
        self.asset_url = asset_url

        # These are the 2 new ones that I'll use standard classes for instead
        self.symbol_map = {}
        self.asset_map = {}

    def get_instruments(self):
        response = requests.get(self.spot_url)
        for item in json.loads(response.text)["result"]:
            if item["type"]=="spot":
                ex_pair_code = item["name"]
                new_symbol = Symbol()
                new_symbol.set_exchange_id("53")
                new_symbol.set_symbol(item["baseCurrency"].lower() + "-" + item["quoteCurrency"].lower())
                new_symbol.set_base_asset(item["baseCurrency"].lower())
                new_symbol.set_quote_asset(item["quoteCurrency"].lower())
                new_symbol.set_instrument_type("spot")
                new_symbol.set_exchange_name("FTX")
                new_symbol.set_exchange_pair_code(ex_pair_code)
                new_symbol.set_expiry('null')
                new_symbol.set_price_prec(get_decimal_places_for_precision(float(item["priceIncrement"])))
                new_symbol.set_qty_prec(get_decimal_places_for_precision(float(item["sizeIncrement"])))
                new_symbol.set_tick_size(float(item["priceIncrement"]))
                new_symbol.set_step_size(float(item["sizeIncrement"]))
                new_symbol.set_contract_size(1.0)
                new_symbol.set_maint_margin(0.0)
                new_symbol.set_required_margin(0.0)
                self.symbol_map[ex_pair_code] = new_symbol

        response = requests.get(self.futures_url)
        for item in json.loads(response.text)["result"]:
            if item["type"] in ["future", "perpetual"]:
                ex_pair_code = item["name"]
                new_symbol = Symbol()
                new_symbol.set_exchange_id("53")
                new_symbol.set_symbol(item["underlying"].lower() + "-" + "usd")
                new_symbol.set_base_asset(item["underlying"].lower())
                new_symbol.set_quote_asset("usd")
                if item["type"] == "future":
                    new_symbol.set_instrument_type("future")
                else:
                    new_symbol.set_instrument_type("perpetual-future")
                new_symbol.set_exchange_name("FTX")
                new_symbol.set_exchange_pair_code(ex_pair_code)
                if item["expiry"] is None:
                    new_symbol.set_expiry('null')
                else:
                    date_tmp = item["expiry"].split("T")[0].split("-")
                    deliveryDate = datetime.datetime(int(date_tmp[0]), int(date_tmp[1]), int(date_tmp[2]))
                    new_symbol.set_expiry(deliveryDate)
                new_symbol.set_price_prec(get_decimal_places_for_precision(float(item["priceIncrement"])))
                new_symbol.set_qty_prec(get_decimal_places_for_precision(float(item["sizeIncrement"])))
                new_symbol.set_tick_size(float(item["priceIncrement"]))
                new_symbol.set_step_size(float(item["sizeIncrement"]))
                new_symbol.set_contract_size(1.0)
                new_symbol.set_maint_margin(0.0)
                new_symbol.set_required_margin(0.0)
                self.symbol_map[ex_pair_code] = new_symbol

 
    def get_assets(self):
        self.asset_map.clear()
        response = requests.get(self.asset_url)
        for item in json.loads(response.text)["result"]:
            if(item.get("underlying","unknown") == "unknown"):
                #  Good - we don't want the leveraged futures created as assets. We can cosider these later if we'd like to go there
                code = item["id"].lower()
                new_asset = Asset()
                new_asset.set_asset_code(code)
                new_asset.set_asset_name(item["name"][0:49])
                new_asset.set_asset_type("unknown")
                self.asset_map[code] = new_asset
