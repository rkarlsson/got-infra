#include "binance_md_process.hpp"

// ##################################################################
// BinanceBaseMDProcessor base class related methods
// ##################################################################

BinanceMDProcessor::BinanceMDProcessor(char *_bin_message_buffer, char *_bin_snapshot_buffer, DecodeResponse *_decode_response, DecodeResponse *_decode_snapshot_response){
    bin_message_buffer      = _bin_message_buffer;
    bin_snapshot_buffer     = _bin_snapshot_buffer;
    decode_response         = _decode_response;
    decode_snapshot_response= _decode_snapshot_response;
    exchange_id             = 0;
    tob_update              = (ToBUpdate *) bin_message_buffer;
    trade_msg               = (Trade *) bin_message_buffer;
    signal_msg              = (Signal *) bin_message_buffer;
};

double BinanceMDProcessor::ascii_to_double( const char * str )
{
    // std::string value = str;
    double final_val = 0;
    double decimal_val = 0;

    int sign_multiplier = 1;
    if (*str == '-') {
        str++;
        sign_multiplier = -1;
    }

    while( *str  && (*str != '.')) {
        final_val = final_val*10 + (*str++ - '0');
    }

    if (*str == '.') {
        str++;
        uint64_t divider = 10;

        while( *str) {
            decimal_val += (double)(*str++ - '0') / (double) divider;
            divider *= 10;
        }    
    }
    return (double)(sign_multiplier * (final_val + decimal_val));    
}

double BinanceMDProcessor::ascii_to_double( const char * str, bool *is_zero )
{
    // std::string value = str;
    double final_val = 0;
    double decimal_val = 0;
    bool _is_zero = true;

    int sign_multiplier = 1;
    if (*str == '-') {
        str++;
        sign_multiplier = -1;
    }

    while( *str  && (*str != '.')) {
        if(*str != '0')
            _is_zero = false;
        final_val = final_val*10 + (*str++ - '0');
    }

    if (*str == '.') {
        str++;
        uint64_t divider = 10;

        while( *str) {
            if(*str != '0')
                _is_zero = false;
            decimal_val += (double)(*str++ - '0') / (double) divider;
            divider *= 10;
        }    
    }
    (*is_zero) = _is_zero;
    return (double)(sign_multiplier * (final_val + decimal_val));    
}

uint64_t BinanceMDProcessor::get_current_ts_ns() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return((t.tv_sec*1000000000L)+t.tv_nsec);
}

void BinanceMDProcessor::process_ticker_message(uint32_t instrument_id) {
    if (md_json_message["T"].error() != simdjson::NO_SUCH_FIELD) {
        exchange_ts = md_json_message["T"].get_uint64() * 1000000;
    } else {
        exchange_ts = _recv_ts - 5000000;
    }
    tob_update->msg_header   = {sizeof(ToBUpdate), TOB_UPDATE, 1};
    tob_update->receive_timestamp       = _recv_ts;
    tob_update->exchange_timestamp      = exchange_ts;
    tob_update->exchange_id             = exchange_id;
    tob_update->instrument_id           = instrument_id;
    tob_update->bid_price               = ascii_to_double(md_json_message["b"].get_c_str());
    tob_update->bid_qty                 = ascii_to_double(md_json_message["B"].get_c_str());
    tob_update->ask_price               = ascii_to_double(md_json_message["a"].get_c_str());
    tob_update->ask_qty                 = ascii_to_double(md_json_message["A"].get_c_str());
}

void BinanceMDProcessor::write_pl_header(uint32_t instrument_id, uint64_t exchange_ts,  uint64_t start_seq, uint64_t end_seq, uint8_t flags){
    //Align pl_details according to where pl_updates is
    pl_details                          = (PriceLevelDetails *) ((char *) pl_updates + sizeof(PLUpdates));

    // Now we can rewrite the pl_updates header to the new memory are
    pl_updates->msg_header              = {sizeof(PLUpdates), PL_UPDATE, 1};            
    pl_updates->receive_timestamp       = _recv_ts;
    pl_updates->exchange_timestamp      = exchange_ts;
    pl_updates->instrument_id           = instrument_id;
    pl_updates->exchange_id             = exchange_id;
    pl_updates->update_flags            = flags;
    pl_updates->start_seq_number        = start_seq;
    pl_updates->end_seq_number          = end_seq;

    // Reset PL counter
    num_of_updates                      = 0;
}

void BinanceMDProcessor::process_details_per_side(uint8_t side, uint32_t instrument_id,uint64_t start_seq, uint64_t end_seq, std::string element) {
    for (auto price_level : md_json_message[element]) {
        int i = 0;
        pl_details->pl_action_type = UPDATE_PL_ACTION;
        pl_details->side = side;
        bool is_zero;
        for (auto value : price_level) {
            if(i==0){
                //price update
                pl_details->price_level         = ascii_to_double(value.get_c_str());
            } else {
                //quantity update
                pl_details->quantity            = ascii_to_double(value.get_c_str(), &is_zero);

                // if qty == 0, set the PL action to delete
                if(is_zero){
                    pl_details->pl_action_type = DELETE_PL_ACTION;
                }
            }
            i++;
        }

        pl_details++;
        num_of_updates++;

        // Check if we need to roll this into another pricelevel update
        if(num_of_updates > PL_UPDATE_MAX_DET_PER_MSG) {
            // Update the return message count
            decode_response->num_messages++;
            decode_snapshot_response->num_messages++;

            // Decrease the number of messages (we have buffered the last so no issues..)
            pl_details--;
            num_of_updates--;

            // Copy last update temporarily into a struct and write to new location later
            PriceLevelDetails temp_pl_det;
            std::memcpy(&temp_pl_det, pl_details, sizeof(PriceLevelDetails));
            // First update the flags of current message to be multipart message and not the last message in the series
            pl_updates->update_flags = PL_UPDATE_MULTIPLE_MESSAGES;

            
            // Then update the final size of this particular message
            total_pl_message_size               = sizeof(PLUpdates) + (num_of_updates * sizeof(PriceLevelDetails));
            pl_updates->msg_header.msgLength    = total_pl_message_size;
            pl_updates->num_of_pl_updates       = num_of_updates;

            // I need to fudge the sequence numbers as we have seq number checker external to this. So need to synthesize
            // TODO - Maybe, might be able to handle this externally as part of the decode_response as it is all encompassing

            // Now we can move to the next message
            char *tmp_ptr                       = (char *) pl_updates;
            pl_updates                          = (PLUpdates *) (tmp_ptr + total_pl_message_size);
            write_pl_header(instrument_id, exchange_ts, start_seq, end_seq, PL_UPDATE_MULTIPLE_MESSAGES | DEFAULT_COMBINED_FLAGS);

            // Copy back the last update to this new depthUpdate
            std::memcpy(pl_details, &temp_pl_det, sizeof(PriceLevelDetails));

            // Increment message counters
            pl_details++;
            num_of_updates++;
        }
    }

}

void BinanceMDProcessor::process_depth_message(uint32_t instrument_id, bool _is_snapshot) {

    uint64_t start_seq;
    uint64_t end_seq;

    // Initialise
    if(_is_snapshot){
        pl_updates                              = (PLUpdates *) bin_snapshot_buffer;
        DEFAULT_COMBINED_FLAGS                  = PL_UPDATE_LAST_MSG_IN_SERIES | PL_UPDATE_SNAPSHOT_MSG;
        start_seq = 0;
        end_seq = md_json_message["lastUpdateId"].get_uint64();
    }
    
    else {
        pl_updates                              = (PLUpdates *) bin_message_buffer;
        DEFAULT_COMBINED_FLAGS                  = PL_UPDATE_LAST_MSG_IN_SERIES;
        start_seq = md_json_message["U"].get_uint64();
        end_seq = md_json_message["u"].get_uint64();
        exchange_ts                                 = md_json_message["E"].get_uint64() * 1000000;
        if (md_json_message["pu"].error() != simdjson::NO_SUCH_FIELD){
            decode_response->previous_end_seq_no    = md_json_message["pu"].get_uint64();
        } else {
            decode_response->previous_end_seq_no    = start_seq - 1;
        }
    }
    
    // Write the header to the memory area
    write_pl_header(instrument_id, exchange_ts, start_seq, end_seq, DEFAULT_COMBINED_FLAGS);

    // Process all bid pricelevel updates
    if(_is_snapshot)
        process_details_per_side(BUY_SIDE, instrument_id, start_seq, end_seq, "bids");
    else
        process_details_per_side(BUY_SIDE, instrument_id, start_seq, end_seq, "b");

    // Process all ask pricelevel updates
    if(_is_snapshot)
        process_details_per_side(SELL_SIDE, instrument_id, start_seq, end_seq, "asks");
    else
        process_details_per_side(SELL_SIDE, instrument_id, start_seq, end_seq, "a");

    total_pl_message_size               = sizeof(PLUpdates) + (num_of_updates * sizeof(PriceLevelDetails));
    pl_updates->msg_header.msgLength    = total_pl_message_size;
    pl_updates->num_of_pl_updates       = num_of_updates;
}


void BinanceMDProcessor::process_message(   std::string_view message, 
                                            uint64_t recv_ts, 
                                            uint32_t instrument_id, 
                                            uint8_t _exchange_id,
                                            double bid_price,
                                            double ask_price,
                                            bool _is_snapshot) {
    _recv_ts = recv_ts;
    exchange_ts = _recv_ts;
    exchange_id = _exchange_id;
    decode_response->num_messages = 1;
    decode_snapshot_response->num_messages  = 1;

    auto result = parser.parse(message.cbegin(), message.length()).get(md_json_message);
    if (result) { 
        return; 
    }    

    // If depth snapshot message - pass on to the depth
    if(_is_snapshot){
        process_depth_message(instrument_id, _is_snapshot);
    } 
    
    else {
        if (md_json_message["e"].error() != simdjson::NO_SUCH_FIELD) {
            std::string_view msg_typ = (std::string_view) md_json_message["e"].get_string();
            // TICKER
            if (msg_typ == "bookTicker") {
                process_ticker_message(instrument_id);
            } 

            // DEPTH
            else if (msg_typ == "depthUpdate") {
                // check return value. if !=0, we need a snapshot as we have a gap
                process_depth_message(instrument_id);
            } 

            // MD_TRADE AGGREGATED
            else if (msg_typ == "aggTrade") {
                exchange_ts = md_json_message["E"].get_uint64() * 1000000;
                trade_msg->msg_header    = {sizeof(Trade), TRADE, 1};
                trade_msg->receive_timestamp         = recv_ts;
                trade_msg->exchange_timestamp        = exchange_ts;
                trade_msg->price                     = ascii_to_double(md_json_message["p"].get_c_str());
                trade_msg->qty                       = ascii_to_double(md_json_message["q"].get_c_str());
                trade_msg->instrument_id             = instrument_id;
                trade_msg->exchange_id               = exchange_id;
                strcpy(trade_msg->exchange_trade_id_first , std::to_string(md_json_message["f"].get_uint64()).c_str());
                strcpy(trade_msg->exchange_trade_id_last , std::to_string(md_json_message["l"].get_uint64()).c_str());
                trade_msg->is_aggregated_trade       = true;
                if(trade_msg->price <= bid_price)
                    trade_msg->side                  = BUY_SIDE;
                else
                    trade_msg->side                  = SELL_SIDE;
            } 
            
            // MD_TRADE SINGLE
            else if (msg_typ == "trade") {
                exchange_ts = md_json_message["E"].get_uint64() * 1000000;
                trade_msg->msg_header    = {sizeof(Trade), TRADE, 1};
                trade_msg->receive_timestamp         = recv_ts;
                trade_msg->exchange_timestamp        = exchange_ts;
                trade_msg->price                     = ascii_to_double(md_json_message["p"].get_c_str());
                trade_msg->qty                       = ascii_to_double(md_json_message["q"].get_c_str());
                trade_msg->instrument_id             = instrument_id;
                trade_msg->exchange_id               = exchange_id;
                strcpy(trade_msg->exchange_trade_id_first , std::to_string(md_json_message["t"].get_uint64()).c_str());
                memset(trade_msg->exchange_trade_id_last , 0, 1);
                trade_msg->is_aggregated_trade       = false;
                if(trade_msg->price <= bid_price)
                    trade_msg->side                  = BUY_SIDE;
                else
                    trade_msg->side                  = SELL_SIDE;
            }

            // FUNDING
            else if (msg_typ == "markPriceUpdate") {
                // Generate fundingrate update
                signal_msg->msg_header    = {sizeof(Signal), SIGNAL, 1};
                signal_msg->receive_timestamp         = recv_ts;
                signal_msg->exchange_timestamp        = md_json_message["T"].get_uint64() * 1000000;
                signal_msg->value                     = ascii_to_double(md_json_message["r"].get_c_str());
                signal_msg->type                      = FUNDING_RATE;
                signal_msg->instrument_id             = instrument_id;
                signal_msg->exchange_id               = exchange_id;
                signal_msg->sequence_nr               = 0;

                // Increase pointer to next message and increate message counter we return
                signal_msg++;
                decode_response->num_messages++;

                // Generate markprice update
                signal_msg->msg_header    = {sizeof(Signal), SIGNAL, 1};
                signal_msg->receive_timestamp         = recv_ts;
                signal_msg->exchange_timestamp        = md_json_message["T"].get_uint64() * 1000000;
                signal_msg->instrument_id             = instrument_id;
                signal_msg->exchange_id               = exchange_id;
                signal_msg->sequence_nr               = 0;            
                signal_msg->type                      = MARK_PRICE;
                signal_msg->value                     = ascii_to_double(md_json_message["p"].get_c_str());

                // decrease message ppointer back to normality
                signal_msg--;
            } 

            // LIQUIDATION
            else if (msg_typ == "forceOrder") {
                // {

                //     "e":"forceOrder",                   // Event Type
                //     "E":1568014460893,                  // Event Time
                //     "o":{

                //         "s":"BTCUSDT",                   // Symbol
                //         "S":"SELL",                      // Side
                //         "o":"LIMIT",                     // Order Type
                //         "f":"IOC",                       // Time in Force
                //         "q":"0.014",                     // Original Quantity
                //         "p":"9910",                      // Price
                //         "ap":"9910",                     // Average Price
                //         "X":"FILLED",                    // Order Status
                //         "l":"0.014",                     // Order Last Filled Quantity
                //         "z":"0.014",                     // Order Filled Accumulated Quantity
                //         "T":1568014460893,              // Order Trade Time

                //     }

                // }

                // Send Liquidation signal
                simdjson::dom::element order_details = md_json_message["o"];
                signal_msg->msg_header    = {sizeof(Signal), SIGNAL, 1};
                signal_msg->receive_timestamp         = recv_ts;
                signal_msg->exchange_timestamp        = order_details["T"].get_uint64() * 1000000;
                signal_msg->value                     = ascii_to_double(order_details["q"].get_c_str()) * ascii_to_double(order_details["ap"].get_c_str());
                signal_msg->type                      = LIQUIDATION_ORDER;
                signal_msg->sequence_nr               = 0;  
                signal_msg->instrument_id             = instrument_id;
                signal_msg->exchange_id               = exchange_id;

                // if(_publish_to_aeron)
                //     to_aeron_io->send_data((char *)&signal_msg, sizeof(Signal));
                // else
                //     bin_file->write_message((char *)&signal_msg, sizeof(Signal));        
            }
        } 
        
        // TICKER - these sometimes don't have a description in them.. hence run again
        else {
            process_ticker_message(instrument_id);
        }
    }
}
