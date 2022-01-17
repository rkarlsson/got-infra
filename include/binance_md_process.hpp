#pragma once
#include <iostream>
#include <string_view>
#include <cmath>
#include <cstring>
#include "to_aeron.hpp"
#include "aeron_types.hpp"
#include "simdjson.h"
#include "double_to_ascii.hpp"
#include "binary_file.hpp"


struct DecodeResponse {
    uint64_t    previous_end_seq_no;
    int         num_messages;
};

// Put all helper and base JSON methods in here which is inherited by the other 
class BinanceMDProcessor {
    private:
        simdjson::dom::parser parser;
        simdjson::dom::element md_json_message;
        simdjson::dom::element current_pl_details;
        char        *bin_message_buffer;
        char        *bin_snapshot_buffer;
        DecodeResponse *decode_response;
        DecodeResponse *decode_snapshot_response;
        uint64_t    exchange_ts;
        uint64_t    _recv_ts;
        uint8_t     exchange_id;

        ToBUpdate   *tob_update;
        Trade       *trade_msg;
        Signal      *signal_msg;
        PLUpdates   *pl_updates;
        PriceLevelDetails   *pl_details;
        uint32_t num_of_updates;

        uint32_t    total_pl_message_size;

        uint8_t DEFAULT_COMBINED_FLAGS;

        double ascii_to_double( const char * str );
        double ascii_to_double( const char * str, bool *is_zero );
        uint64_t get_current_ts_ns();
        void process_ticker_message(uint32_t instrument_id);
        void process_depth_message(uint32_t instrument_id, bool _is_snapshot = false);
        void write_pl_header(uint32_t instrument_id, uint64_t exchange_ts, uint64_t start_seq, uint64_t end_seq, uint8_t flags);
        void process_details_per_side(uint8_t side, uint32_t instrument_id,uint64_t start_seq, uint64_t end_seq,std::string element);
        void populate_pl_details();

    public:

        BinanceMDProcessor(char *_bin_message_buffer, char *_bin_snapshot_buffer, DecodeResponse *_decode_response, DecodeResponse *_decode_snapshot_response);

        void process_message(   std::string_view message, 
                                uint64_t recv_ts, 
                                uint32_t instrument_id, 
                                uint8_t exchange_id,
                                double bid_price,
                                double ask_price,
                                bool _is_snapshot = false);

};
