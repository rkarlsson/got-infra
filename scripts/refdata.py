#!/usr/bin/env python3

import mysql.connector as mysql
import argparse

from modules.refdata_tools import RefDBAccess, Symbol
from modules.binance_tools import BinanceJSONParse
from modules.ftx_tools import FTXJSONParse
from modules.kraken_tools import KrakenJSONParse


def BinanceProcessing(exchange_name, url, dry_run, check_live):
    exchange_inst = BinanceJSONParse(url, exchange_name)
    db_inst = RefDBAccess(exchange_name, exchange_inst)
    db_inst.get_all_symbols_from_db()

    a=set(db_inst.items_from_db.keys())
    b=set(exchange_inst.instruments_from_json.keys())

    if check_live:
        in_db_not_json = a-b

        for i in in_db_not_json:
            cmd = None
            if db_inst.items_from_db[i][db_inst.columns["InstrumentType"]]=="future":
                if db_inst.items_from_db[i][db_inst.columns["Live"]] == 1:
                    cmd = "UPDATE Instrument SET Live = 2 where InstrumentID = %d;" % (db_inst.items_from_db[i][db_inst.columns["InstrumentID"]])
            else:
                if db_inst.items_from_db[i][db_inst.columns["Live"]] == 1:
                    cmd = "UPDATE Instrument SET Live = 3 where InstrumentID = %d;" % (db_inst.items_from_db[i][db_inst.columns["InstrumentID"]])
            if cmd is not None:
                print(cmd)
                if not dry_run:
                    db_inst.execute(cmd)


    else:
        to_add = b-a
        print("To add(%s): %s"%(exchange_name, repr(to_add)))
        for item in to_add:
            db_inst.insert_instrument(item, dry_run)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Synchronizes reference data from exchange with our own database")
    parser.add_argument('--exchange', dest='exchange', default="Binance")
    parser.add_argument('--check_db_instruments', dest='check_live', action="store_true")
    parser.add_argument('--dry_run', dest='dry_run', action="store_true")
    parser.add_argument('--download_assets', dest='download_assets', action="store_true" )
    args = parser.parse_args()

    ################################################
    # Step 1 - Get all data from RefDB
    ################################################
    refdb = RefDBAccess()
    refdb.get_all_symbols(False)
    refdb.get_all_assets()

    ################################################
    # Step 2 - Get all exchange Symbol and Asset info
    ################################################
    # FTX
    ftx_refdata = FTXJSONParse("https://ftx.com/api/markets", "https://ftx.com/api/futures", "https://ftx.com/api/coins")
    ftx_refdata.get_instruments()
    ftx_refdata.get_assets()
    #Binance Spot
    binance_spot_refdata = BinanceJSONParse("https://api.binance.com/api/v3/exchangeInfo", "Binance")
    binance_spot_refdata.get_instruments()
    binance_spot_refdata.get_assets()
    #Binance Futures
    binance_futures_refdata = BinanceJSONParse("https://fapi.binance.com/fapi/v1/exchangeInfo", "Binance Futures")
    binance_futures_refdata.get_instruments()
    binance_futures_refdata.get_assets()
    # Binance DEX
    binance_dex_refdata = BinanceJSONParse("https://dapi.binance.com/dapi/v1/exchangeInfo", "BinanceDEX")
    binance_dex_refdata.get_instruments()
    binance_dex_refdata.get_assets()
    # Kraken
    kraken_refdata = KrakenJSONParse("https://api.kraken.com/0/public/AssetPairs")
    kraken_refdata.get_assets()

    ################################################
    # Step 3 - Add assets to database that is missing. This takes some fiddling as I need to fill in voids
    ################################################
    all_assets = {}
    all_assets.update(ftx_refdata.asset_map)
    for key,value in kraken_refdata.asset_map.items():
        if(key in all_assets):
            if(all_assets[key].type == "unknown"):
                if (value.type != "" and value.type != "unknown"):
                    all_assets[key].type = value.type
                else:
                    all_assets[key].type = "cryptocurrency"
        else:
            all_assets[key] = value

    for key,value in binance_spot_refdata.asset_map.items():
        if(key in all_assets):
            if(all_assets[key].type == "unknown"):
                if (value.type != "" and value.type != "unknown"):
                    all_assets[key].type = value.type
                else:
                    all_assets[key].type = "cryptocurrency"
        else:
            all_assets[key] = value

    for key,value in binance_futures_refdata.asset_map.items():
        if(key in all_assets):
            if(all_assets[key].type == "unknown"):
                if (value.type != "" and value.type != "unknown"):
                    all_assets[key].type = value.type
                else:
                    all_assets[key].type = "cryptocurrency"
        else:
            all_assets[key] = value

    for key,value in binance_dex_refdata.asset_map.items():
        if(key in all_assets):
            if(all_assets[key].type == "unknown"):
                if (value.type != "" and value.type != "unknown"):
                    all_assets[key].type = value.type
                else:
                    all_assets[key].type = "cryptocurrency"
        else:
            all_assets[key] = value

    for asset in all_assets.values():
        if asset.type == "unknown":
            asset.type = "cryptocurrency"
        if asset.type == "cryptocurrency":
            asset.type_id = "3"
        elif asset.type == "fiat":
            asset.type_id = "1"
        elif asset.type == "commodity":
            asset.type_id = "2"
        elif asset.type == "stablecoin":
            asset.type_id = "5"
        elif asset.type == "leveraged token":
            asset.type_id = "4"

    missing_assets = set(all_assets.keys()) - set(refdb.asset_map_code.keys())

    refdb.close_and_reopen()

    for asset in missing_assets:
        print("Code: %s, Name: %s, Type: %s, Type_ID: %s"%(all_assets[asset].code, all_assets[asset].name, all_assets[asset].type, all_assets[asset].type_id))
        refdb.insert_asset(all_assets[asset], args.dry_run)

    ################################################
    # Step 4 - Add Instruments to database now when all assets should be there
    ################################################

    # Re-run the asset query from the DB - as that's the true source to confirm if I can add a symbol or not
    refdb.get_all_assets()
    asset_keys = refdb.asset_map_code.keys()
    instruments_keys = refdb.symbol_map_paircode.keys()
    for new_instrument in binance_dex_refdata.symbol_map.values():
        # Check that it isn't already in the database
        if(new_instrument.exchange_pair_code not in instruments_keys):
            # Check that both quote and base assets is in database
            if(new_instrument.base_asset in asset_keys and new_instrument.quote_asset in asset_keys):
                # Adding symbol
                refdb.insert_instrument(new_instrument, args.dry_run)
            else:
                print("%s has %s and %s"%(new_instrument.exchange_pair_code, new_instrument.base_asset, new_instrument.quote_asset))

    refdb.close()
