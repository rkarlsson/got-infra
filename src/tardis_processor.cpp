#include "tardis_processor.hpp"
#include <cstring>

TardisProcessor::TardisProcessor(char *_msg_ptr, char _data_type, uint8_t _exchange_id, uint32_t _instrument_id){
    data_type = _data_type;
    exchange_id = _exchange_id;
    instrument_id = _instrument_id;

    if(data_type == 't'){ // ToB
        tob_update = (ToBUpdate *) _msg_ptr;
        tob_update->msg_header.msgLength = sizeof(ToBUpdate);
        tob_update->msg_header.msgType = TOB_UPDATE;
        tob_update->msg_header.protoVersion = 1;
        tob_update->exchange_id = exchange_id;
        tob_update->instrument_id = instrument_id;
    } else if(data_type == 'T'){ // Trades
        trade_msg = (Trade *) _msg_ptr;
        trade_msg->msg_header.msgLength = sizeof(Trade);
        trade_msg->msg_header.msgType = TRADE;
        trade_msg->msg_header.protoVersion = 1;
        trade_msg->exchange_id = exchange_id;
        trade_msg->instrument_id = instrument_id;     
    } else if(data_type == 'p'){ // PLUpdate
        pl_updates = (PLUpdates *) _msg_ptr;
        pl_updates->msg_header.msgLength = sizeof(PLUpdates);
        pl_updates->msg_header.msgType = PL_UPDATE;
        pl_updates->msg_header.protoVersion = 1;
        pl_updates->exchange_id = exchange_id;
        pl_updates->instrument_id = instrument_id;
        pl_updates->start_seq_number = 0;
        pl_updates->end_seq_number = 0;
        in_pl_loop = false;
    }
    msg_ptr = _msg_ptr; // In case need in the future as well
}

bool TardisProcessor::add_price_level(){
    //exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
    pl_details++; // increment pl_details to the second one
    pl_updates->msg_header.msgLength += sizeof(PriceLevelDetails);
    pl_updates->num_of_pl_updates++;

    // Skip is_snapshot
    while (*curr_ptr != ','){
        curr_ptr++;
    }
    curr_ptr++;

    // Side
    if(strncmp (curr_ptr,"ask",3) == 0)
        pl_details->side = SELL_SIDE;
    else
        pl_details->side = BUY_SIDE;

    while (*curr_ptr != ','){
        curr_ptr++;
    }
    curr_ptr++;

    // Price
    double temp_val = 0.0;
    uint64_t divider = 10;
    while (*curr_ptr != ',' && *curr_ptr != '.'){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    pl_details->price_level = temp_val;
    curr_ptr++;

    // Quantity
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != '\n' && *curr_ptr != '.' && *curr_ptr != 0){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != 0) {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    pl_details->quantity = temp_val;

    // Set update type
    if(pl_details->quantity < 0.0000000001)
        pl_details->pl_action_type = DELETE_PL_ACTION;
    else
        pl_details->pl_action_type = UPDATE_PL_ACTION;

    return(true);
}

bool TardisProcessor::process_plupdate(){
    //exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
    pl_updates->msg_header.msgLength = sizeof(PLUpdates) + sizeof(PriceLevelDetails);
    pl_updates->exchange_timestamp = exchange_ts;
    pl_updates->receive_timestamp = recv_ts;
    pl_updates->sending_timestamp = recv_ts + 2500;
    pl_updates->num_of_pl_updates = 1;
    pl_details = (PriceLevelDetails *) (((char *)pl_updates) + sizeof(struct PLUpdates)); // reset to the first one

    // Is Snapshot? (true/false)
    if(strncmp (curr_ptr,"true",4) == 0)
        pl_updates->update_flags = PL_UPDATE_SNAPSHOT_MSG + PL_UPDATE_LAST_MSG_IN_SERIES;
    else
        pl_updates->update_flags = PL_UPDATE_LAST_MSG_IN_SERIES;

    while (*curr_ptr != ','){
        curr_ptr++;
    }
    curr_ptr++;

    // Side
    if(strncmp (curr_ptr,"ask",3) == 0)
        pl_details->side = SELL_SIDE;
    else
        pl_details->side = BUY_SIDE;

    while (*curr_ptr != ','){
        curr_ptr++;
    }
    curr_ptr++;

    // Price
    double temp_val = 0.0;
    uint64_t divider = 10;
    while (*curr_ptr != ',' && *curr_ptr != '.'){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    pl_details->price_level = temp_val;
    curr_ptr++;

    // Quantity
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != '\n' && *curr_ptr != '.' && *curr_ptr != 0){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != 0) {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    pl_details->quantity = temp_val;
    if(pl_details->quantity < 0.0000000001)
        pl_details->pl_action_type = DELETE_PL_ACTION;
    else
        pl_details->pl_action_type = UPDATE_PL_ACTION;
    return(true);
}

bool TardisProcessor::process_tob(){
    //exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount
    tob_update->exchange_timestamp = exchange_ts;
    tob_update->receive_timestamp = recv_ts;
    tob_update->sending_timestamp = recv_ts + 2500;

    // Ask Quantity
    double temp_val = 0.0;
    uint64_t divider = 10;
    while (*curr_ptr != ',' && *curr_ptr != '.'){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    tob_update->ask_qty = temp_val;
    curr_ptr++;

    // Ask Price
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != ',' && *curr_ptr != '.'){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    tob_update->ask_price = temp_val;
    curr_ptr++;

    // Bid Price
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != ',' && *curr_ptr != '.'){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    tob_update->bid_price = temp_val;
    curr_ptr++;

    // Bid Amount
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != '\n' && *curr_ptr != '.' && *curr_ptr != 0){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;        
        // do the decimals
        while( *curr_ptr != 0) {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    tob_update->bid_qty = temp_val;
    return(true);
}

bool TardisProcessor::process_trade(){
    // exchange,symbol,timestamp,local_timestamp,id,side,price,amount
    trade_msg->exchange_timestamp = exchange_ts;
    trade_msg->receive_timestamp = recv_ts;
    trade_msg->sending_timestamp = recv_ts + 2500;

    // Trade ID
    double temp_val = 0.0;
    uint64_t divider = 10;
    uint8_t length = 0;
    char *start_pos = curr_ptr;
    while (*curr_ptr != ','){
        length++;
        curr_ptr++;
    }
    memcpy(trade_msg->exchange_trade_id_first, start_pos, length);
    trade_msg->exchange_trade_id_first[length] = 0;
    memcpy(trade_msg->exchange_trade_id_last, start_pos, length);
    trade_msg->exchange_trade_id_last[length] = 0;
    curr_ptr++;

    // Side
    if(strncmp (curr_ptr,"buy",3) == 0)
        trade_msg->side = BUY_SIDE;
    else
        trade_msg->side = SELL_SIDE;

    while (*curr_ptr != ','){
        curr_ptr++;
    }
    curr_ptr++;

    // Trade Price
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != '.' && *curr_ptr != ','){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;
        // do the decimals
        while( *curr_ptr != ',') {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    trade_msg->price = temp_val;
    curr_ptr++;

    // Trade Quantity
    temp_val = 0.0;
    divider = 10;
    while (*curr_ptr != '\n' && *curr_ptr != '.' && *curr_ptr != 0){
        temp_val *= 10;
        temp_val += *curr_ptr - '0';
        curr_ptr++;
    }

    if(*curr_ptr == '.'){
        curr_ptr++;
        // do the decimals
        while( *curr_ptr != 0) {
            temp_val += (double)(*curr_ptr - '0') / (double) divider;
            divider *= 10;
            curr_ptr++;
        }  
    }
    trade_msg->qty = temp_val;

    return(true);
}

bool TardisProcessor::process_message(char *line_ptr){
    // reset
    exchange_ts = 0;
    recv_ts = 0;

    curr_ptr = line_ptr;
    // Exchangename
    while (*curr_ptr != ',')
        curr_ptr++;
    curr_ptr++;

    // Instrumentname
    while (*curr_ptr != ',')
        curr_ptr++;
    curr_ptr++;

    // Exchange timestamp
    while (*curr_ptr != ','){
        exchange_ts *= 10;
        exchange_ts += *curr_ptr -'0';
        curr_ptr++;
    }
    exchange_ts *= 1000; // microseconds in tardis data
    curr_ptr++;

    // receive timestamp
    while (*curr_ptr != ','){
        recv_ts *= 10;
        recv_ts += *curr_ptr -'0';
        curr_ptr++; 
    }
    recv_ts *= 1000; // microseconds in tardis data
    curr_ptr++;

    if(data_type == 't'){
        return(process_tob());
    }
    else if (data_type == 'T'){
        return(process_trade());
    }
    else if (data_type == 'p'){
        // Check if exchange timestamp is the same as previous PL Update and then combine them
        if(exchange_ts == pl_updates->exchange_timestamp){
            // Add another pricelevel_detail to message
            add_price_level();
            return(false);
        }
        if(in_pl_loop) {
            // nice we can now write this to the binary file - return true and reset in_pl_loop
            in_pl_loop = false;
            exchange_ts = 0;
            return(true);
        }
        in_pl_loop = true;
        process_plupdate();
        return(false);
    }
    return(false);
}