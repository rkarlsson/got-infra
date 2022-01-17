#pragma once
#include <iostream>
#include <string_view>
#include "to_aeron.hpp"
#include "aeron_types.hpp"
#include "simdjson.h"
#include "double_to_ascii.hpp"


// Put all helper and base JSON methods in here which is inherited by the other 
class KrakenMDProcessor {
    private:
        simdjson::dom::parser parser;
        simdjson::dom::element md_json_message;
        to_aeron    *to_aeron_io;
        uint64_t    exchange_ts;
        uint64_t    _recv_ts;
        uint8_t     exchange_id;
        ToBUpdate   tob_update;
        Trade       trade_msg;
        Signal      signal_msg;


        double ascii_to_double( const char * str );
        uint64_t get_current_ts_ns();
        void process_ticker_message(uint32_t instrument_id);

    public:

        KrakenMDProcessor(){
            exchange_id = 0;
            to_aeron_io = new to_aeron(AERON_IO);
            tob_update.msg_header   = {sizeof(ToBUpdate), TOB_UPDATE, 1};
            trade_msg.msg_header    = {sizeof(Trade), TRADE, 1};
            trade_msg.side          = UNKNOWN_SIDE;
            signal_msg.msg_header   = {sizeof(Signal), SIGNAL, 1};
            signal_msg.sequence_nr  = 0;
        };

        void process_message(std::string_view message, uint64_t recv_ts, uint32_t instrument_id, uint8_t exchange_id);
};
