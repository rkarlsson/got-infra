#!/usr/bin/env python3

import mysql.connector as mysql
from datetime import date

# from scripts.process_captured_data import get_all_symbols_from_db

class Symbol:
    def __init__(self):
        self.symbol = ""
        self.exchange_pair_code = ""
        self.instrument_id = ""
        self.exchange_id = ""
        self.price_precision = 0
        self.quantity_precision = 0
        self.tick_size = 0.0
        self.step_size = 0.0
        self.contract_size = 0.0
        self.maint_margin = 0.0
        self.required_margin = 0.0
        self.exchange_name = ""
        self.expiry = ""
        self.base_asset = ""
        self.quote_asset = ""
        self.instrument_type = ""

    def set_instrument_id(self, instrument_id):
        self.instrument_id = instrument_id

    def set_exchange_id(self, exchange_id):
        self.exchange_id = exchange_id

    def set_symbol(self, symbol):
        self.symbol = symbol.lower() # This should always be in lowercase

    def set_exchange_pair_code(self, exchange_pair_code):
        self.exchange_pair_code = exchange_pair_code

    def set_expiry(self, expiry):
        self.expiry = expiry

    def set_base_asset(self, base_asset):
        self.base_asset = base_asset

    def set_quote_asset(self, quote_asset):
        self.quote_asset = quote_asset

    def set_instrument_type(self, instrument_type):
        self.instrument_type = instrument_type

    def set_tick_size(self, tick):
        self.tick_size = tick

    def set_step_size(self, step):
        self.step_size = step

    def set_contract_size(self, con_size):
        self.contract_size = con_size

    def set_maint_margin(self, m_margin):
        self.maint_margin = m_margin
    
    def set_required_margin(self, r_margin):
        self.required_margin = r_margin

    def set_price_prec(self, price_prec):
        self.price_precision = price_prec

    def set_qty_prec(self, qty_prec):
        self.quantity_precision = qty_prec

    def set_exchange_name(self, exchange_name):
        self.exchange_name = exchange_name

class Asset:
    def __init__(self):
        self.code = ""
        self.name = ""
        self.type = ""
        self.asset_id = ""
        self.type_id = ""

    def set_asset_id(self, id):
        self.id = str(id)

    def set_asset_name(self, name):
        self.name = name

    def set_asset_code(self, code):
        self.code = code

    def set_asset_type(self, type):
        self.type = type

    def set_asset_type_id(self, type_id):
        self.type_id = str(type_id)

class RefDBAccess:
    def __init__(self):
        self.db = mysql.connect(host="18.183.96.89", user="GoT_user", password="GoTCrypto", database="ReferenceData", autocommit=True)
        self.cursor = self.db.cursor()
        # These are the 2 new ones that I'll use standard classes for instead
        self.symbol_map_id = {}
        self.symbol_map_paircode = {}
        self.exchangeid_map_list = {} # Map exchange IDs to a list for each ID with all symbols for that exchange ID
        self.exchangename_map_list = {} # Map exchange Names to a list for each ID with all symbols for that exchange ID
        self.asset_map_id = {}
        self.asset_map_code = {}

    def close_and_reopen(self):
        self.cursor.close()
        self.cursor = self.db.cursor()

    def close(self):
        self.cursor.close()
        self.db.close()        

    def execute(self, query):
        self.cursor.execute(query)
        # self.db.commit()

    def get_all_symbols(self, only_live_instruments):
        field_list = "InstrumentID,ExchangeID,PricePrecision,QuantityPrecision,TickSize,StepSize,ContractSize"
        field_list += ",MaintMarginPercent,RequiredMarginPercent,Exchange,ExchangePairCode"
        my_query = "SELECT " + field_list + " FROM ReferenceData.vwInstrument WHERE Live < "
        if(only_live_instruments == True):
            my_query += "2"
        else:
            my_query += "4"

        self.execute(my_query)
        response = self.cursor.fetchall()

        for instrument in response:
            sym = Symbol()
            sym.set_instrument_id(str(instrument[0]))
            sym.set_exchange_id(str(instrument[1]))
            if(instrument[2] is not None):
                sym.set_price_prec(instrument[2])
            if(instrument[3] is not None):
                sym.set_qty_prec(instrument[3])
            if(instrument[4] is not None):
                sym.set_tick_size(instrument[4])
            if(instrument[5] is not None):
                sym.set_step_size(instrument[5])
            if(instrument[6] is not None):
                sym.set_contract_size(instrument[6])
            if(instrument[7] is not None):
                sym.set_maint_margin(instrument[7])
            if(instrument[8] is not None):
                sym.set_required_margin(instrument[8])
            if(instrument[9] is not None):
                sym.set_exchange_name(instrument[9])
            if(instrument[10] is not None):
                sym.set_exchange_pair_code(instrument[10])                                

            self.symbol_map_id[sym.instrument_id] = sym
            self.symbol_map_paircode[sym.exchange_pair_code] = sym

            # Per exchange ID
            if(sym.exchange_id not in self.exchangeid_map_list):
                self.exchangeid_map_list[sym.exchange_id] = []
            self.exchangeid_map_list[sym.exchange_id].append(sym)

            # Per Exchange name
            if(sym.exchange_name not in self.exchangename_map_list):
                self.exchangename_map_list[sym.exchange_name] = []
            self.exchangename_map_list[sym.exchange_name].append(sym)

    def get_all_assets(self):
        # Clear the maps in case I re-run the query
        self.asset_map_id = {}
        self.asset_map_code = {}

        field_list = "AssetID,Code,Name,AssetType,fAssetTypeID"
        my_query = "SELECT " + field_list + " FROM ReferenceData.vwAsset"
        self.execute(my_query)
        response = self.cursor.fetchall()

        for asset in response:
            new_asset = Asset()
            new_asset.set_asset_id(str(asset[0]))
            new_asset.set_asset_code(asset[1])
            new_asset.set_asset_name(asset[2])
            new_asset.set_asset_type(asset[3])
            new_asset.set_asset_type_id(asset[4])
            self.asset_map_id[new_asset.asset_id] = new_asset
            self.asset_map_code[new_asset.code] = new_asset

    def insert_instrument(self, sym_to_insert, dry_run):
        arg_tuple = (
                sym_to_insert.symbol,
                sym_to_insert.instrument_type,
                sym_to_insert.exchange_name,
                sym_to_insert.exchange_pair_code,
                sym_to_insert.base_asset,
                sym_to_insert.quote_asset,
                sym_to_insert.expiry,
                f"{sym_to_insert.price_precision:d}",
                f"{sym_to_insert.quantity_precision:d}",
                f"{sym_to_insert.tick_size:.8f}",
                f"{sym_to_insert.step_size:.8f}",
                f"{sym_to_insert.contract_size:.8f}",
                f"{sym_to_insert.maint_margin:.5f}",
                f"{sym_to_insert.required_margin:.5f}")
        print(arg_tuple)

        if not dry_run:
            try:
                result_args = self.cursor.callproc('sp_InstrumentInsert', arg_tuple)
            except mysql.Error as err:
                print("Something went wrong with instrument insert: {}".format(err))
            print(result_args)

    def insert_asset(self, asset_to_insert, dry_run):
        arg_tuple = (
                str(asset_to_insert.code),
                str(asset_to_insert.name),
                asset_to_insert.type)
        print(arg_tuple)

        if not dry_run:
            try:
                result_args = self.cursor.callproc('sp_AssetInsert', arg_tuple)
            except mysql.Error as err:
                print("Something went wrong with asset insert: {}".format(err))
            print(result_args)
