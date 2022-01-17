#pragma once
#include <iostream>
#include "aeron_types.hpp"


// Use this to parse Tardis updates
class TardisProcessor {
    private:
        char        *msg_ptr;
        char        *curr_ptr;
        char        data_type;
        uint8_t     exchange_id;
        uint32_t    instrument_id;

        ToBUpdate   *tob_update;
        Trade       *trade_msg;
        PLUpdates   *pl_updates;
        PriceLevelDetails   *pl_details;
        InstrumentClearBook instrument_clear_msg;

        uint64_t    exchange_ts;
        uint64_t    recv_ts;
        bool        in_pl_loop;

        double      ascii_to_double( const char * str );

        bool        process_tob();
        bool        process_trade();
        bool        process_plupdate();
        bool        add_price_level();

    public:
        TardisProcessor(char *_msg_ptr, char _data_type, uint8_t _exchange_id, uint32_t _instrument_id);
        bool        process_message(char *line_ptr);
};
