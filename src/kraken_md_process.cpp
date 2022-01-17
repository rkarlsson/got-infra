#include "kraken_md_process.hpp"

// ##################################################################
// BinanceBaseMDProcessor base class related methods
// ##################################################################
double KrakenMDProcessor::ascii_to_double( const char * str )
{
    std::string value = str;
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
    return (sign_multiplier * (final_val + decimal_val));    
}

uint64_t KrakenMDProcessor::get_current_ts_ns() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return((t.tv_sec*1000000000L)+t.tv_nsec);
}

void KrakenMDProcessor::process_ticker_message(uint32_t instrument_id) {
    if (md_json_message["T"].error() != simdjson::NO_SUCH_FIELD) {
        exchange_ts = md_json_message["T"].get_uint64() * 1000000;
    }

    tob_update.receive_timestamp        = _recv_ts;
    tob_update.exchange_timestamp       = exchange_ts;
    tob_update.instrument_id            = instrument_id;
    tob_update.bid_price                = ascii_to_double(md_json_message["b"].get_c_str());
    tob_update.bid_qty                  = ascii_to_double(md_json_message["B"].get_c_str());
    tob_update.ask_price                = ascii_to_double(md_json_message["a"].get_c_str());
    tob_update.ask_qty                  = ascii_to_double(md_json_message["A"].get_c_str());
    tob_update.instrument_id            = instrument_id;
    tob_update.exchange_id              = exchange_id;
    tob_update.sending_timestamp = get_current_ts_ns();
    to_aeron_io->send_data((char *)&tob_update, sizeof(ToBUpdate));
}

void KrakenMDProcessor::process_message(std::string_view message, uint64_t recv_ts, uint32_t instrument_id, uint8_t _exchange_id) {
    _recv_ts = recv_ts;
    exchange_ts = _recv_ts;
    exchange_id = _exchange_id;
    auto result = parser.parse(message.cbegin(), message.length()).get(md_json_message);
    if (result) { 
        // std::stringstream error_message; 
        // error_message << result;
        // logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
        return; 
    }    

    if (md_json_message["e"].error() != simdjson::NO_SUCH_FIELD) {
        // MD_TRADE AGGREGATED
        if ((std::string_view) md_json_message["e"].get_string() == "aggTrade") {
            exchange_ts = md_json_message["E"].get_uint64() * 1000000;
            trade_msg.receive_timestamp         = recv_ts;
            trade_msg.exchange_timestamp        = exchange_ts;
            trade_msg.price                     = ascii_to_double(md_json_message["p"].get_c_str());
            trade_msg.qty                       = ascii_to_double(md_json_message["q"].get_c_str());
            trade_msg.instrument_id             = instrument_id;
            trade_msg.exchange_id               = exchange_id;
            strcpy(trade_msg.exchange_trade_id_first , std::to_string(md_json_message["f"].get_uint64()).c_str());
            strcpy(trade_msg.exchange_trade_id_last , std::to_string(md_json_message["l"].get_uint64()).c_str());
            trade_msg.is_aggregated_trade       = true;
            trade_msg.sending_timestamp = get_current_ts_ns();
            to_aeron_io->send_data((char *)&trade_msg, sizeof(Trade));
        } 
        
        // MD_TRADE SINGLE
        else if ((std::string_view) md_json_message["e"].get_string() == "trade") {
            exchange_ts = md_json_message["E"].get_uint64() * 1000000;
            trade_msg.receive_timestamp         = recv_ts;
            trade_msg.exchange_timestamp        = exchange_ts;
            trade_msg.price                     = ascii_to_double(md_json_message["p"].get_c_str());
            trade_msg.qty                       = ascii_to_double(md_json_message["q"].get_c_str());
            trade_msg.instrument_id             = instrument_id;
            trade_msg.exchange_id               = exchange_id;
            strcpy(trade_msg.exchange_trade_id_first , std::to_string(md_json_message["t"].get_uint64()).c_str());
            memset(trade_msg.exchange_trade_id_last , 0, 1);
            trade_msg.is_aggregated_trade       = false;

            trade_msg.sending_timestamp = get_current_ts_ns();
            to_aeron_io->send_data((char *)&trade_msg, sizeof(Trade));
        }

        // TICKER
        else if ((std::string_view) md_json_message["e"].get_string() == "bookTicker") {
            process_ticker_message(instrument_id);
        } 

        // FUNDING
        else if ((std::string_view) md_json_message["e"].get_string() == "markPriceUpdate") {
            uint64_t exchange_timestamp = md_json_message["T"].get_uint64() * 1000000;

            // Send fundingrate update
            signal_msg.receive_timestamp         = recv_ts;
            signal_msg.exchange_timestamp        = exchange_ts;
            signal_msg.value                     = ascii_to_double(md_json_message["r"].get_c_str());
            signal_msg.type                      = FUNDING_RATE;
            signal_msg.instrument_id             = instrument_id;
            signal_msg.exchange_id               = exchange_id;

            to_aeron_io->send_data((char *)&signal_msg, sizeof(Signal));

            // Send markprice update
            signal_msg.type                      = MARK_PRICE;
            signal_msg.value                     = ascii_to_double(md_json_message["p"].get_c_str());

            to_aeron_io->send_data((char *)&signal_msg, sizeof(Signal));
        } 

        // // INCR
        // else if ((std::string_view) md_json_message["e"].get_string() == "depthUpdate") {
        //     exchange_ts = md_json_message["E"].get_uint64() * 1000000;
        //     serial = md_json_message["U"].get_uint64();
        
        // } 

        // // LIQUIDATION
        // else if ((std::string_view) md_json_message["e"].get_string() == "forceOrder") {
        
        // }
    } 
    
    // TICKER
    else {
        process_ticker_message(instrument_id);
    }
}

