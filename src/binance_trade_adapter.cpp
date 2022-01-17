#include "binance_trade_adapter.hpp"
#include <boost/algorithm/string.hpp>



// =================================================================================
// Retrieves the initial listenkey for a private user websocket
// =================================================================================
std::string BinanceTradeAdapter::get_listen_key(std::string rest_endpoint, uint8_t exchange_id) {
    // Get the listenkey
    CURL *curl;
    std::string listenKey;
    uint64_t refresh_listen_key_time;
    std::string response;
    std::string tmp_url;
    simdjson::dom::parser parser;
    if (exchange_id == 16) {
        tmp_url = rest_endpoint + "userDataStream";
    } else {
        tmp_url = rest_endpoint + "listenKey";
    } 

    curl = curl_easy_init();
    struct curl_slist *chunk = NULL;
    std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
    chunk = curl_slist_append(chunk, hdrs.c_str());
    curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
    CURLcode rc = curl_easy_perform(curl);
    if (rc) {
        logger->msg(ERROR, curl_easy_strerror(rc));
        return ("");
    }
    logger->msg(INFO, "Listenkey request response=" + response);

    simdjson::dom::element response_json;
    auto error = parser.parse(response.c_str(), response.length()).get(response_json);
    if (error) { 
        std::stringstream error_text;
        error_text << error;
        logger->msg(ERROR, error_text.str());
        return(""); 
    }
    if (response_json["listenKey"].error() != simdjson::NO_SUCH_FIELD) {
        listenKey = as_string(response_json["listenKey"]);
        refresh_listen_key_time = get_current_ts() + (30*60*1000000000L);
    } else {
        logger->msg(ERROR, "Didn't receive a listenkey in the curl response");
        return("");
    }
    return(listenKey);
}

// =================================================================================
// ListenKey refresh thread - all keys in the key_map are renewed every x minutes
// =================================================================================
void BinanceTradeAdapter::listenkey_refresh_thread(Logger *keylogger) {

    std::thread listener_thread([this, keylogger]() {
        CURL *curl;

        curl = curl_easy_init();
        uint64_t key_time = get_current_ts();

        keylogger->msg(INFO, "Started refresh thread (for listenkeys)");

        while(1) {
            sleep(1);
            if (get_current_ts() > key_time) {
                // Loop through all websockets and renew all of the keys
                for (auto map_entry : listenkey_map) {
                    auto ws_info = map_entry.second;
                    keylogger->msg(INFO, "refresh listen key");
                    std::string response;
                    std::string tmp_url;
                    if (ws_info->exchange_name == "Binance") {
                        std::string post_data;
                        std::stringstream body;
                        std::string body_str;
                        body << "listenKey=" << ws_info->listenKey;
                        body_str = body.str();
                        post_data = "?" + body_str;
                        tmp_url = ws_info->rest_endpoint + "userDataStream" + post_data;
                    } else {
                        tmp_url = ws_info->rest_endpoint + "listenKey";
                    } 
                    keylogger->msg(INFO, "URL = " + tmp_url);
                    struct curl_slist *chunk = NULL;
                    std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                    chunk = curl_slist_append(chunk, hdrs.c_str());
                    curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
                    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
                    CURLcode rc = curl_easy_perform(curl);
                    if (rc) {
                        keylogger->msg(ERROR, curl_easy_strerror(rc));
                        return false;
                    }
                    keylogger->msg(INFO, "ListenKey refresh response for exchange: " + ws_info->exchange_name + " = " + response);
                }
                key_time = get_current_ts() + (30*60*1000000000L);
            }
        }
    });
    listener_thread.detach();
}

// =================================================================================
// This thread connects to the websocket of binance and gets all account related updates
// =================================================================================
void BinanceTradeAdapter::add_user_websocket(Logger *logger, uint8_t exchange_id, std::string ex_name) {
    logger->msg(INFO, "Adding websocket for exchangeID: " + std::to_string(exchange_id));
    // Get the listenkey and add to the key_map
    listenkey_map[exchange_id]->listenKey = get_listen_key(listenkey_map[exchange_id]->rest_endpoint, exchange_id);
    // open socket to the exchange by adding the subcription thread
    user_websockets->add_subscription_request(  listenkey_map[exchange_id]->ws_endpoint + listenkey_map[exchange_id]->listenKey, 
                                                nullptr,
                                                "not_used",
                                                0, 
                                                exchange_id);
}

// =================================================================================
// This method starts a thread and loops around any messages from all exchange user websockets
// =================================================================================
void BinanceTradeAdapter::user_websocket_loop_thread(Logger *wslogger) {
    std::thread user_ws_loop_thread([this, wslogger]() {
        std::string_view ws_message;
        uint8_t exchange_id;
        uint64_t recv_ts;

        // message loop
        while (1) {
            // This one will wait around epoll_wait
            ws_message = user_websockets->get_next_message_from_websocket();

            // From which exchange did we receive the message and what timestamps
            exchange_id = user_websockets->get_exchange_id();
            recv_ts = user_websockets->get_message_receive_time();

            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            uint64_t current_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
            uint64_t sent_ts;

            simdjson::dom::element exchange_json_message;
            simdjson::dom::element order_details;
            // Parsing with SIMDJSON parser
            auto error = parser.parse(ws_message.cbegin(), ws_message.length()).get(exchange_json_message);
            if (error) { 
                std::stringstream error_message; 
                error_message << error;
                wslogger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                return; 
            }

            // We won't process any messages that doesn't contain an "e" (Event Type)
            if (exchange_json_message["e"].error() != simdjson::NO_SUCH_FIELD) {

                // ORDER UPDATE
                if (    ((std::string_view) exchange_json_message["e"].get_string() == "ORDER_TRADE_UPDATE") || // Futures
                        ((std::string_view) exchange_json_message["e"].get_string() == "executionReport"))      // Spot
                {
                    // Normalise exchange response into order_details for easier handling
                    if ((std::string_view) exchange_json_message["e"].get_string() == "ORDER_TRADE_UPDATE")
                        order_details = exchange_json_message["o"];
                    else
                        order_details = exchange_json_message;

                    wslogger->msg(INFO, "WebSocket Message=" + std::string(ws_message.cbegin(), ws_message.length()));
                    // Extract order details from the exchange message
                    std::string_view response_order_id = order_details["c"].get_string();
                    char external_order_id_temp[MAX_EXTERNAL_ORDER_ID_LENGTH];
                    strcpy(external_order_id_temp, order_details["c"].get_c_str());
                    bool isbid = ((std::string_view) order_details["S"].get_string() == "BUY");
                    bool is_liquidation_order = ((std::string_view) order_details["o"].get_string() == "LIQUIDATION");

                    // Handle MARKET / LIMIT order updates (this includes web initiated orders and Liquidation orders)
                    if (    ((std::string_view) order_details["o"].get_string() == "LIMIT") ||
                            ((std::string_view) order_details["o"].get_string() == "MARKET") ||
                            is_liquidation_order)
                    {
                        // NEW ORDER MESSAGE
                        if((std::string_view) order_details["X"].get_string() == "NEW") {

                            // {"e":"executionReport",
                            // "E":1624649424612,
                            // "s":"BTCUSDT",
                            // "c":"7zgSmQ9C1",
                            // "S":"BUY",
                            // "o":"LIMIT",
                            // "f":"GTC",
                            // "q":"0.00100000",
                            // "p":"31765.38000000",
                            // "P":"0.00000000",
                            // "F":"0.00000000",
                            // "g":-1,
                            // "C":"",
                            // "x":"NEW",
                            // "X":"NEW",
                            // "r":"NONE",
                            // "i":6622961854,
                            // "l":"0.00000000",
                            // "z":"0.00000000",
                            // "L":"0.00000000",
                            // "n":"0",
                            // "N":null,
                            // "T":1624649424611,
                            // "t":-1,
                            // "I":14156327846,
                            // "w":true,
                            // "m":false,
                            // "M":false,
                            // "O":1624649424611,
                            // "Z":"0.00000000",
                            // "Y":"0.00000000",
                            // "Q":"0.00000000"}                            

                            // WEB INITIATED ORDER OR LIQUIDATION ORDERS
                            // We synthesize an order/ack/exchange ack onto the bus so we can create new records for it
                            if ((response_order_id.rfind("web_", 0) == 0) || (is_liquidation_order)) {
                                // Create new order with internal order_id generated
                                uint64_t temp_location_id = 1;
                                uint64_t top_8_bits = get_order_env();
                                top_8_bits <<= 56;
                                temp_location_id <<= 48;

                                struct SendOrder *s = new SendOrder();
                                s->msg_header.msgType = MSG_NEW_ORDER;
                                s->msg_header.msgLength = sizeof(struct SendOrder);
                                s->msg_header.protoVersion = 1;
                                s->is_buy     = isbid;
                                s->price      = ascii_to_double(order_details["p"].get_c_str());
                                s->qty        = ascii_to_double(order_details["q"].get_c_str());
                                s->instrument_id   = instr_name_to_instr_id[as_string(order_details["s"])];
                                s->internal_order_id = get_next_washbook_internal_order_id(std::to_string(order_details["i"].get_uint64()), exchange_id); // This gets the next available order id and adds to map
                                s->exchange_id   = exchange_id;
                                s->strategy_id   = 0; // Strategy 0 is used as wash book for forced updates
                                if(is_liquidation_order)
                                    s->order_type   = FORCED_LIQUIDATION;
                                else
                                    s->order_type   = MANUAL_ORDER;
                                send_any_io_message((char *)s, sizeof(struct SendOrder));
                                // Save order details for future use
                                exchange_order_id_to_order[std::to_string(order_details["i"].get_uint64())] = s;


                                // SENDING INTERNAL ACK
                                struct RequestAck request_message;
                                request_message.msg_header.msgType = MSG_REQUEST_ACK;
                                request_message.msg_header.msgLength = sizeof(struct RequestAck);
                                request_message.msg_header.protoVersion = 1;
                                request_message.internal_order_id = s->internal_order_id;
                                strcpy(request_message.external_order_id, order_details["i"].get_c_str());
                                request_message.instrument_id = s->instrument_id;
                                request_message.exchange_id = exchange_id;
                                request_message.strategy_id = 0;
                                request_message.ack_type = REQUEST_ACK;
                                request_message.reject_reason = UNKNOWN_REJECT;
                                request_message.reject_message[0] = '\0';
                                send_any_io_message((char *)&request_message, sizeof(request_message));

                                // NOW SEND THE EXCHANGE ACK
                                request_message.ack_type = EXCHANGE_ACK;
                                send_any_io_message((char *)&request_message, sizeof(request_message));

                            }

                            // JUST AN ACK FROM OWN INITIATED ORDER
                            else 
                            {
                                clock_gettime(CLOCK_REALTIME, &t);
                                sent_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
                                send_exchange_order_ack(external_order_id_temp);
                                std::string exchange_order_id = std::to_string(order_details["i"].get_uint64());
                                wslogger->msg(INFO, "Got Exchange Ack for exchange orderID: " + exchange_order_id);


                                auto iter = external_order_id_to_order.find(external_order_id_temp);
                                if(iter != external_order_id_to_order.end()){
                                    wslogger->log_ts(OE_READER_THREAD, iter->second->internal_order_id, current_ts, sent_ts);
                                    // make a link to the order details for the exchange order id for future updates
                                    exchange_order_id_to_order[exchange_order_id] = external_order_id_to_order[std::string(external_order_id_temp)];
                                } else {
                                    wslogger->log_ts(OE_READER_THREAD, 0, current_ts, sent_ts);
                                }
                            }
                        }
                        
                        // CANCEL ORDER
                        else if (((std::string_view) order_details["X"].get_string() == "CANCELED") ||
                                ((std::string_view) order_details["X"].get_string() == "EXPIRED")) {

                            if (response_order_id.rfind("web_", 0) == 0) {
                                // Handle a cancel or expire from web interface
                                // We need to synthesize a cancel request and then send the internal cancel ack and exchange cancel ack

                                // websocket_messageloop_thread:WebSocket Message={ "e":"executionReport",
                                                                                // "E":1624627518904,
                                                                                // "s":"BTCUSDT",
                                                                                // "c":"web_df8506846d754dc7b0fdd22ebcece41a",
                                                                                // "S":"BUY",
                                                                                // "o":"LIMIT",
                                                                                // "f":"GTC",
                                                                                // "q":"0.00100000",
                                                                                // "p":"32245.73000000",
                                                                                // "P":"0.00000000",
                                                                                // "F":"0.00000000",
                                                                                // "g":-1,
                                                                                // "C":"5hjl9lcA0",
                                                                                // "x":"CANCELED",
                                                                                // "X":"CANCELED",
                                                                                // "r":"NONE",
                                                                                // "i":6617694906,
                                                                                // "l":"0.00000000",
                                                                                // "z":"0.00000000",
                                                                                // "L":"0.00000000",
                                                                                // "n":"0",
                                                                                // "N":null,
                                                                                // "T":1624627518904,
                                                                                // "t":-1,
                                                                                // "I":14145177253,
                                                                                // "w":false,
                                                                                // "m":false,
                                                                                // "M":false,
                                                                                // "O":1624627501404,
                                                                                // "Z":"0.00000000",
                                                                                // "Y":"0.00000000",
                                                                                // "Q":"0.00000000"}

                                // Check if the exchange_order_id is in our map
                                bool web_cancel_of_existing_order = false;
                                std::string external_order_id;
                                external_order_id = std::to_string(order_details["i"].get_uint64());

                                if(exchange_order_id_to_order.count(external_order_id) > 0)
                                    web_cancel_of_existing_order = true;

                                // SENDING CANCEL REQ
                                struct CancelOrder cancel_message;
                                cancel_message.msg_header.msgType = MSG_CANCEL_ORDER;
                                cancel_message.msg_header.msgLength = sizeof(struct CancelOrder);
                                cancel_message.msg_header.protoVersion = 1;
                                cancel_message.exchange_id = exchange_id;
                                cancel_message.cancel_type = WEB_CANCEL;

                                if(web_cancel_of_existing_order){
                                    cancel_message.internal_order_id = exchange_order_id_to_order[external_order_id]->internal_order_id;                                
                                    cancel_message.instrument_id = instr_name_to_instr_id[as_string(order_details["s"])];
                                }
                                else {
                                    cancel_message.internal_order_id = 0;
                                }
                                cancel_message.strategy_id = 0;
                                send_any_io_message((char *)&cancel_message, sizeof(struct CancelOrder));

                                // SENDING INTERNAL ACK
                                struct RequestAck request_message;
                                request_message.msg_header.msgType = MSG_REQUEST_ACK;
                                request_message.msg_header.msgLength = sizeof(struct RequestAck);
                                request_message.msg_header.protoVersion = 1;
                                request_message.internal_order_id = cancel_message.internal_order_id;
                                if(web_cancel_of_existing_order){
                                    strcpy(request_message.external_order_id, internal_order_id_to_external_order_id[request_message.internal_order_id].c_str());
                                }
                                else {
                                    strcpy(request_message.external_order_id, order_details["c"].get_c_str()); // should never occurr
                                }

                                request_message.instrument_id = cancel_message.instrument_id;
                                request_message.exchange_id = exchange_id;
                                request_message.strategy_id = 0;
                                request_message.ack_type = CANCEL_REQUEST_ACK;
                                request_message.reject_reason = UNKNOWN_REJECT;
                                request_message.reject_message[0] = '\0';
                                send_any_io_message((char *)&request_message, sizeof(struct RequestAck));

                                // NOW SEND THE EXCHANGE CANCEL ACK
                                request_message.ack_type = EXCHANGE_CANCEL_ACK;
                                send_any_io_message((char *)&request_message, sizeof(struct RequestAck));
                            } 
                            //Not Web order - internally sent
                            else {
                                std::string order;
                                if (exchange_id == 16) // id = binance
                                    order = as_string(order_details["C"]);
                                else
                                    order = as_string(order_details["c"]);

                                clock_gettime(CLOCK_REALTIME, &t);
                                sent_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
                                send_exchange_cancel_ack((char *) order.c_str());
                                wslogger->msg(INFO, "Got Exchange Cancel Ack for: " + order);
                                auto iter = external_order_id_to_internal_order_id.find(order);
                                if(iter != external_order_id_to_internal_order_id.end()){
                                    wslogger->log_ts(OE_READER_THREAD, iter->second, current_ts, sent_ts);
                                } else {
                                    wslogger->log_ts(OE_READER_THREAD, 0, current_ts, sent_ts);
                                }                                
                            }
                        } 
                        
                        // FILL
                        else if (   ((std::string_view) order_details["X"].get_string() == "PARTIALLY_FILLED") ||
                                    ((std::string_view) order_details["X"].get_string() == "FILLED")) {
                            // Pre-calcluate leaves_qty for all
                            double leaves_qty = ascii_to_double(order_details["q"].get_c_str()) - ascii_to_double(order_details["z"].get_c_str());

                            // Commission details extracted - same across all trades
                            double commission_fee = ascii_to_double(order_details["n"].get_c_str());
                            uint32_t commission_asset_id = 0;
                            std::string asset_lc = boost::algorithm::to_lower_copy(as_string(order_details["N"]));
                            if (base_name_to_asset_id.count(asset_lc)) {
                                commission_asset_id = base_name_to_asset_id[asset_lc];
                            }
                            TradingFee trading_fee;
                            trading_fee.msg_header = MessageHeaderT{sizeof(struct TradingFee), TRADING_FEE, 1};
                            trading_fee.commission_fee = commission_fee;
                            trading_fee.asset_id = commission_asset_id;
                            trading_fee.fee_type = EXECUTION_FEE;
                            trading_fee.exchange_id = exchange_id;
                            strcpy(trading_fee.exchange_trade_id , std::to_string(static_cast<uint64_t>(order_details["t"].get_uint64())).c_str());

                            // WEB OR LIQUIDATION FILLS
                            if ((response_order_id.rfind("web_", 0) == 0) || (is_liquidation_order)) {
                                wslogger->msg(INFO, "Got FILL on web initiated order");
                                SendOrder *s = exchange_order_id_to_order[std::to_string(order_details["i"].get_uint64())];

                                Fill fill_message;
                                fill_message.msg_header = MessageHeaderT{sizeof(Fill), MSG_FILL, 0};
                                fill_message.fill_price                = ascii_to_double(order_details["L"].get_c_str());
                                fill_message.fill_qty                  = ascii_to_double(order_details["l"].get_c_str());
                                strcpy((char *) fill_message.external_order_id, order_details["i"].get_c_str());
                                fill_message.exchange_id               = exchange_id;
                                strcpy(fill_message.exchange_trade_id , std::to_string(static_cast<uint64_t>(order_details["t"].get_uint64())).c_str());
                                fill_message.internal_order_id         = s->internal_order_id;

                                if(fill_message.internal_order_id == 0) {
                                    wslogger->msg(INFO, "Something went wrong with the lookup of the internal order id from the external when mapping the fill");
                                }

                                fill_message.instrument_id             = s->instrument_id;
                                fill_message.strategy_id               = 0; // washbook strategy id
                                fill_message.leaves_qty                = leaves_qty;
                                send_any_io_message((char *)&fill_message, sizeof(fill_message));

                                // Also send the trading fee
                                trading_fee.internal_order_id = fill_message.internal_order_id;
                                send_any_io_message((char *)&trading_fee, sizeof(trading_fee));
                            } 
                            
                            // NORMAL FILL
                            else 
                            {
                                clock_gettime(CLOCK_REALTIME, &t);
                                sent_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
                                send_fill(  external_order_id_temp, 
                                            ascii_to_double(order_details["L"].get_c_str()), 
                                            ascii_to_double(order_details["l"].get_c_str()),
                                            leaves_qty,
                                            isbid,
                                            std::to_string(static_cast<uint64_t>(order_details["t"].get_uint64())));
                                wslogger->msg(INFO, "Got FILL");
                                auto iter = external_order_id_to_internal_order_id.find(external_order_id_temp);
                                if(iter != external_order_id_to_internal_order_id.end()){
                                    // Also send the trading fee (only do this when finding a corresponding internal order id)
                                    trading_fee.internal_order_id = iter->second;
                                    send_any_io_message((char *)&trading_fee, sizeof(trading_fee));
                                    // log timestamp
                                    wslogger->log_ts(OE_READER_THREAD, iter->second, current_ts, sent_ts);
                                } else {
                                    wslogger->log_ts(OE_READER_THREAD, 0, current_ts, sent_ts);
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    user_ws_loop_thread.detach();
}

// =================================================================================
// This handles all new orders/cancels from the message bus
// =================================================================================
fragment_handler_t BinanceTradeAdapter::aeron_msg_handler() {
    return
        [&](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
            // Take current time first of all
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            uint64_t current_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
            char *msg_ptr = reinterpret_cast<char *>(buffer.buffer()) + offset;

            struct MessageHeader *m = (MessageHeader*) msg_ptr;

            switch(m->msgType) {
                case MSG_NEW_ORDER: {
                    struct SendOrder *s = (struct SendOrder*) msg_ptr;
                    // Check that the order is intended for this exchange_id
                    if( ((s->exchange_id < 16) || (s->exchange_id > 18)) || 
                        (s->order_type == FORCED_LIQUIDATION) || 
                        (s->order_type == MANUAL_ORDER)) 
                    {
                        break;
                    }

                    std::string reject_message;
                    uint8_t reject_reason;
                    char ext_order_id[MAX_EXTERNAL_ORDER_ID_LENGTH];
                    std::string rest_endpoint = listenkey_map[s->exchange_id]->rest_endpoint;

                    // Initialise new external orderid
                    strncpy((char *)ext_order_id, oe_unique_order_id.c_str(), unique_part_for_order_id);
                    snprintf((char *) ext_order_id + unique_part_for_order_id, 9, "%d", external_order_id);
                    std::string order_id = ext_order_id;

                    if (order_pass_riskcheck(s, reject_message, reject_reason)) 
                    {
                        // SUCCESFUL RISKCHECKS

                        // Persisting the order details
                        external_order_id_to_internal_order_id[order_id] = s->internal_order_id;
                        internal_order_id_to_external_order_id[s->internal_order_id] = order_id;
                        external_order_id++;                        
                        auto new_order = new struct SendOrder();
                        *new_order = *s;
                        internal_order_id_to_order[s->internal_order_id] = new_order;
                        external_order_id_to_order[order_id] = new_order;

                        // send aeron internal ack that riskcecks went well - needed the external order id above
                        send_internal_order_ack(ext_order_id, s);

                        char order_uri[512];
                        memset(order_uri, 0, 512); // this makes it safe that it always have a termination
                        char *uri_ptr = order_uri;
                        char *body_ptr;
                        memcpy(order_uri, rest_endpoint.c_str(), rest_endpoint.length());
                        uri_ptr += rest_endpoint.length();
                        memcpy(uri_ptr, "order?", 6);
                        uri_ptr += 6;
                        body_ptr = uri_ptr; // this is used for signature creation

                        auto *instrument = instrument_info_map[s->instrument_id];

                        memcpy(uri_ptr, "symbol=", 7);
                        uri_ptr += 7;

                        memcpy(uri_ptr, instrument->instrument_name, strlen(instrument->instrument_name));
                        uri_ptr += strlen(instrument->instrument_name);
                       
                        if (s->is_buy) {
                            memcpy(uri_ptr, "&side=BUY&type=LIMIT&timeInForce=GTC&quantity=", 46);
                            uri_ptr+=46;
                        } else {
                            memcpy(uri_ptr, "&side=SELL&type=LIMIT&timeInForce=GTC&quantity=", 47);
                            uri_ptr+=47;
                        }

                        if (instrument->qty_precision != 0) { 
                            uri_ptr += double_to_ascii(s->qty, uri_ptr, instrument->qty_precision);
                        } else {
                            uri_ptr += double_to_ascii(s->qty, uri_ptr, 8);
                        }

                        memcpy(uri_ptr, "&price=", 7);
                        uri_ptr+=7;

                        if (instrument->price_precision != 0) {
                            uri_ptr += double_to_ascii(s->price, uri_ptr, instrument->price_precision);
                        } else {
                            uri_ptr += double_to_ascii(s->price, uri_ptr, 8);
                        }

                        memcpy(uri_ptr, "&newClientOrderId=", 18);
                        uri_ptr+=18;

                        memcpy(uri_ptr, ext_order_id, strlen(ext_order_id));
                        uri_ptr += strlen(ext_order_id);

                        memcpy(uri_ptr, "&recvWindow=5000&timestamp=", 27);
                        uri_ptr+=27;
                        
                        memcpy(uri_ptr, std::to_string(get_current_ts_millis()).c_str(), 13); // copies 13 characters of millis in
                        uri_ptr += 13;

                        uri_ptr += hmacHex(SECRET_KEY, body_ptr, uri_ptr);

                        curl_easy_setopt(new_order_curl, CURLOPT_URL, order_uri);
                        
                        // Get timestamp as close to sending the order as possible
                        clock_gettime(CLOCK_REALTIME, &t);
                        send_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
                        CURLcode rc = curl_easy_perform(new_order_curl);

                        logger->msg(INFO, "URL: " + std::string(order_uri, strlen(order_uri)));
                        logger->msg(INFO, "NewOrder response=" + new_order_response);
                        // Now check the response
                        if (rc) {
                            std::string error_message = curl_easy_strerror(rc);
                            logger->msg(ERROR, "Error in NewOrder: " + error_message);
                            return;
                        }
                        logger->log_ts(AERON_OE_THREAD, s->internal_order_id, current_ts, send_ts);

                        simdjson::dom::element exchange_json_message;
                        auto error = parser.parse(new_order_response.c_str(), new_order_response.length()).get(exchange_json_message);
                        new_order_response.clear();
                        if (error) { 
                            std::stringstream error_message; 
                            error_message << error;
                            logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                            break; 
                        }
                        if( (exchange_json_message["code"].error() != simdjson::NO_SUCH_FIELD) && 
                            (exchange_json_message["msg"].error() != simdjson::NO_SUCH_FIELD)) {
                            // Any message back from binance with "code" tag is a exchange reject
                            send_exchange_order_reject(   ext_order_id, 
                                                    as_string(exchange_json_message["msg"]), 
                                                    EXCHANGE_REJECT);
                            
                            logger->msg(INFO, "Got Exchange New Order Reject for: " + order_id); 
                        } 
                    }
                    else 
                    {
                        // FAILED RISKCHECKS
                        send_internal_order_reject(s, reject_message, reject_reason);
                    }
                }
                break;

                case MSG_CANCEL_ORDER: {
                    struct CancelOrder *c = (struct CancelOrder*) msg_ptr;
                    if((c->exchange_id < 16) || (c->exchange_id > 18) || (c->cancel_type == WEB_CANCEL)){
                        break;
                    }
                    if(internal_order_id_to_external_order_id.count(c->internal_order_id)){
                        std::string post_data;
                        std::string body;
                        std::string cancel_external_order_id = internal_order_id_to_external_order_id[c->internal_order_id];
                        std::string rest_endpoint = listenkey_map[c->exchange_id]->rest_endpoint;
                        char url_char_array[512];
                        char *url_end_ptr = url_char_array + 7; // Length of "symbol="
                        memset(url_char_array, 0, 512);

                        // Internal Cancel ack
                        send_internal_cancel_ack(c, (char *)cancel_external_order_id.c_str());

                        std::string symbol_name = instr_id_to_instr_name[c->instrument_id];
                        memcpy(url_char_array, "symbol=", 7);
                        memcpy(url_end_ptr, symbol_name.c_str(), symbol_name.length());
                        url_end_ptr += symbol_name.length();

                        if (c->exchange_id == 16) {
                            memcpy(url_end_ptr, "&origClientOrderId=", 19);
                            url_end_ptr += 19;
                        } else {
                            memcpy(url_end_ptr, "&type=DELETE&origClientOrderId=", 31);
                            url_end_ptr += 31;
                        }
                        memcpy(url_end_ptr, cancel_external_order_id.c_str(), cancel_external_order_id.length());
                        url_end_ptr += cancel_external_order_id.length();

                        memcpy(url_end_ptr, "&recvWindow=5000&timestamp=", 27);
                        url_end_ptr+=27;

                        body = url_char_array + std::to_string(get_current_ts_millis());

                        std::string signature = hmacHex(SECRET_KEY, body);
                        post_data = "?" + body + "&signature=" + signature;
                        std::string tmp_url = rest_endpoint + "order" + post_data;
                        curl_easy_setopt(cancel_curl, CURLOPT_URL, tmp_url.c_str());

                        // cancel timestamp first as close as possible to source
                        clock_gettime(CLOCK_REALTIME, &t);
                        send_ts = (t.tv_sec*1000000000L) + t.tv_nsec;
                        CURLcode rc = curl_easy_perform(cancel_curl);

                        // First log all that has happened (delayed until after curl call)
                        logger->msg(INFO, "URL: " + tmp_url);
                        logger->msg(INFO, "Cancel response=" + cancel_response);
                        logger->log_ts(AERON_OE_THREAD, c->internal_order_id, current_ts, send_ts);

                        // Now check response code
                        if (rc) {
                            std::string error_message = curl_easy_strerror(rc);
                            logger->msg(ERROR, "Error in Cancel: " + error_message);
                            return;
                        }

                        simdjson::dom::element exchange_json_message;
                        auto error = parser.parse(cancel_response.c_str(), cancel_response.length()).get(exchange_json_message);
                        cancel_response.clear();
                        if (error) { 
                            std::stringstream error_message; 
                            error_message << error;
                            logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                            break; 
                        }

                        if( (exchange_json_message["code"].error() != simdjson::NO_SUCH_FIELD) && 
                            (exchange_json_message["msg"].error() != simdjson::NO_SUCH_FIELD)) {

                            send_exchange_cancel_reject(    c,
                                                            (char *)cancel_external_order_id.c_str(), 
                                                            as_string(exchange_json_message["msg"]),
                                                            EXCHANGE_CANCEL_REJECT_REASON);

                            logger->msg(INFO, "Got Exchange Cancel Reject for: " + cancel_external_order_id);
                        }
                    } 
                    else 
                    {
                        // We couldn't find the order..
                        send_internal_cancel_reject(c, "Could not find any order with that ID", INTERNAL_ORDER_ID_NOT_FOUND);
                    }
                }
                break;

                case HEARTBEAT_REQUEST: {
                        got_hb.store(true, std::memory_order_release);
                        struct HeartbeatResponse h = HeartbeatResponse{   MessageHeaderT{sizeof(HeartbeatResponse), HEARTBEAT_RESPONSE, 0}, 
                                                                            EXECUTION_SERVICE, 
                                                                            1, 
                                                                            0};
                        to_aeron_io->send_data((char*)&h, sizeof(h));
                    }
                    break;

                case INSTRUMENT_INFO_RESPONSE: {
                        struct InstrumentInfoResponse *instrument = (InstrumentInfoResponse*) msg_ptr;
                        if((instrument->exchange_id >= 16) && (instrument->exchange_id <= 18)){
                            update_instrument_info(instrument);
                        }
                    }
                    break;

                case STRATEGY_INFO_RESPONSE:{ 
                        struct StrategyInfoResponse *strat_info = (StrategyInfoResponse*) msg_ptr;
                        update_strategy_info(strat_info);
                    }
                    break;

                case ACCOUNT_INFO_REQUEST: {
                    AccountInfoRequest *account_info = (AccountInfoRequest *) m;
                    if((account_info->exchange_id < 16) || (account_info->exchange_id > 18)){
                        break;
                    }
                    switch(account_info->update_type_reason) {
                        case FUNDING_FEE:{
                            // Binance spot do not have support for this
                            if(account_info->exchange_id != 16) {
                                std::string post_data;
                                std::stringstream body;
                                std::string body_str;
                                std::string response;
                                std::string rest_endpoint = listenkey_map[account_info->exchange_id]->rest_endpoint;

                                uint64_t ts_ms = get_current_ts()/1000000;
                                logger->msg(INFO, "Retrieved new funding fee request from ringbuffer");
                                body << "incomeType=FUNDING_FEE&recvWindow=5000&timestamp=" << ts_ms;
                                body_str = body.str();
                                std::string signature = hmacHex(SECRET_KEY, body_str);
                                post_data = "?" + body_str + "&signature=" + signature;
                                std::string tmp_url = rest_endpoint + "income" + post_data;
                                logger->msg(INFO, "URL: " + tmp_url);
                                struct curl_slist *chunk = NULL;
                                std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                                chunk = curl_slist_append(chunk, hdrs.c_str());
                                curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                                curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
                                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                                CURLcode rc = curl_easy_perform(curl);
                                if (rc) {
                                    logger->msg(ERROR, curl_easy_strerror(rc));
                                    return;
                                }
                                logger->msg(INFO, "funding_fee response = " + response);
                                if(response.length() >2 ) {
                                    simdjson::dom::element exchange_json_message;
                                    auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                                    if (error) { 
                                        std::stringstream error_message; 
                                        error_message << error;
                                        logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                                        return; 
                                    }
                                    uint64_t from_time = ts_ms - (10*60*1000);

                                    for (simdjson::dom::element response_element : exchange_json_message) {
                                        std::string asset_lc = boost::algorithm::to_lower_copy(as_string(response_element["asset"]));

                                        uint32_t asset_id = 0;
                                        if (base_name_to_asset_id.count(asset_lc)) {
                                            asset_id = base_name_to_asset_id[asset_lc];
                                        }

                                        uint32_t instrument_id = 0;
                                        if (instr_name_to_instr_id.count(as_string(response_element["symbol"]))) {
                                            instrument_id = instr_name_to_instr_id[as_string(response_element["symbol"])];
                                        }

                                        // uint64_t tmp_id = (((uint64_t)asset_id)<<32)+((uint64_t)instrument_id);
                                        uint64_t time = response_element["time"].get_uint64();

                                        if (time > from_time) {
                                            std::stringstream income_string;
                                            income_string << "Income for asset " << response_element["asset"];
                                            income_string << " (asset id=" << asset_id << ") and instrument " << response_element["symbol"];
                                            income_string << " (instrument_id=" << instrument_id << ")" <<  " value = " << response_element["income"];
                                            logger->msg(INFO, income_string.str());
                                            send_account_update(    asset_id,
                                                                                instrument_id,
                                                                                ascii_to_double(response_element["income"].get_c_str()),
                                                                                INCOME_UPDATE,
                                                                                FUNDING_FEE,
                                                                                account_info->exchange_id);
                                        }
                                    }
                                } else {
                                    logger->msg(INFO, "Length of funding fee response was not more than 2 bytes, nothing to process");
                                }
                            } else {
                                logger->msg(INFO, "Funding request not supported for Binance Spot, ignoring");
                            }
                        }
                        break;

                        case POSITION_RISK_REQUEST:{
                            // Binance spot do not have support for this
                            if(account_info->exchange_id != 16) {
                                std::string post_data;
                                std::stringstream body;
                                std::string body_str;
                                std::string response;
                                std::string rest_endpoint = listenkey_map[account_info->exchange_id]->rest_endpoint;
                                logger->msg(INFO, "Retrieved new position request from ringbuffer");
                                body << "recvWindow=5000&timestamp=" << get_current_ts()/1000000; 
                                body_str = body.str();
                                std::string signature = hmacHex(SECRET_KEY, body_str);
                                post_data = "?" + body_str + "&signature=" + signature;
                                std::string tmp_url = rest_endpoint + "positionRisk" + post_data;
                                logger->msg(INFO, "URL: " + tmp_url);
                                struct curl_slist *chunk = NULL;
                                std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                                chunk = curl_slist_append(chunk, hdrs.c_str());
                                curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                                curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
                                CURLcode rc = curl_easy_perform(curl);
                                if (rc) {
                                    logger->msg(ERROR, curl_easy_strerror(rc));
                                    return;
                                }

                                logger->msg(INFO, "position request response=" + response);

                                simdjson::dom::element exchange_json_message;
                                auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                                if (error) { 
                                    std::stringstream error_message; 
                                    error_message << error;
                                    logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                                    return; 
                                }

                                for (simdjson::dom::element position_element : exchange_json_message) {
                                    if (instr_name_to_instr_id.count(as_string(position_element["symbol"]))) {
                                        uint32_t instrument_id = instr_name_to_instr_id[as_string(position_element["symbol"])];
                                        // logger->msg(INFO, "Sending position update for: " + as_string(position_element["symbol"]));
                                        send_account_update( 0, 
                                                                    instrument_id,
                                                                    ascii_to_double(position_element["positionAmt"].get_c_str()),
                                                                    INSTRUMENT_POSITION,
                                                                    POSITION_RISK_REQUEST,
                                                                    account_info->exchange_id);
                                    }
                                }
                            }
                        }
                        break;

                        case ACCOUNT_INFO:{
                            std::string post_data;
                            std::stringstream body;
                            std::string body_str;
                            std::string response;
                            std::string rest_endpoint = listenkey_map[account_info->exchange_id]->rest_endpoint;
                            logger->msg(INFO, "Retrieved new account request from ringbuffer");
                            body << "recvWindow=5000&timestamp=" << get_current_ts()/1000000; 
                            body_str = body.str();
                            std::string signature = hmacHex(SECRET_KEY, body_str);
                            post_data = "?" + body_str + "&signature=" + signature;
                            std::string tmp_url = rest_endpoint + "account" + post_data;
                            logger->msg(INFO, "URL: " + tmp_url);
                            struct curl_slist *chunk = NULL;
                            std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                            chunk = curl_slist_append(chunk, hdrs.c_str());
                            curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
                            CURLcode rc = curl_easy_perform(curl);
                            if (rc) {
                                logger->msg(ERROR, curl_easy_strerror(rc));
                                return;
                            }
                            logger->msg(INFO, "account response = " + response);
                            simdjson::dom::element exchange_json_message;
                            auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                            if (error) { 
                                std::stringstream error_message; 
                                error_message << error;
                                logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                                return; 
                            }

                            // Only Binance Spot has balances
                            if (exchange_json_message["balances"].error() != simdjson::NO_SUCH_FIELD) {
                                logger->msg(INFO, "Got Account Reponse for Balances (only SPOT has balance updates, synthetically creating a position with USDT behind it)");
                                for (simdjson::dom::element balance : exchange_json_message["balances"]) {
                                    // logger->msg(INFO, "Balance update for asset " + as_string(balance["asset"]));
                                    std::string asset_lc = boost::algorithm::to_lower_copy(as_string(balance["asset"]));
                                    if (base_name_to_asset_id.count(asset_lc)) {
                                        uint32_t asset_id = base_name_to_asset_id[asset_lc];
                                        send_account_update(  asset_id, 
                                                                        0, 
                                                                        ascii_to_double(balance["free"].get_c_str()), 
                                                                        WALLET_BALANCE,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                        if ( (std::string_view) balance["asset"].get_string() != "GBP"){
                                            send_account_update(  asset_id, 
                                                                        0, 
                                                                        ascii_to_double(balance["free"].get_c_str()), 
                                                                        INSTRUMENT_POSITION,
                                                                        UNKNOWN_REASON,
                                                                        account_info->exchange_id);
                                        }
                                    }
                                }
                            }

                            // Binance Futures and DEX
                            if (exchange_json_message["assets"].error() != simdjson::NO_SUCH_FIELD) {
                                logger->msg(INFO, "Got Account Reponse for Assets");

                                for (simdjson::dom::element asset : exchange_json_message["assets"]) {
                                    // logger->msg(INFO, "Asset update for asset: " + as_string(asset["asset"]));
                                    std::string asset_lc = boost::algorithm::to_lower_copy(as_string(asset["asset"]));
                                    if (base_name_to_asset_id.count(asset_lc)) {
                                        uint32_t asset_id = base_name_to_asset_id[asset_lc];
                                        send_account_update(  asset_id, 
                                                                        0, 
                                                                        ascii_to_double(asset["walletBalance"].get_c_str()), 
                                                                        WALLET_BALANCE,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                        send_account_update(  asset_id, 
                                                                        0, 
                                                                        ascii_to_double(asset["crossWalletBalance"].get_c_str()), 
                                                                        CROSS_MARGIN_BALANCE,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                        send_account_update(  asset_id, 
                                                                        0, 
                                                                        ascii_to_double(asset["initialMargin"].get_c_str()), 
                                                                        INITIAL_MARGIN,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                    }
                                }
                            }

                            // Binance Futures and DEX
                            if (exchange_json_message["positions"].error() != simdjson::NO_SUCH_FIELD) {        
                                logger->msg(INFO, "Got Account Reponse for Positions");

                                for (simdjson::dom::element position : exchange_json_message["positions"]) {
                                    // logger->msg(INFO, "Position update or instrument " + as_string(position["symbol"]));

                                    if (position["positionAmt"].error() != simdjson::NO_SUCH_FIELD) {        
                                        send_account_update(  0, 
                                                                        instr_name_to_instr_id[as_string(position["symbol"])], 
                                                                        ascii_to_double(position["positionAmt"].get_c_str()), 
                                                                        INSTRUMENT_POSITION,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                    }
                                    if (position["unrealizedProfit"].error() != simdjson::NO_SUCH_FIELD) {        
                                        send_account_update(  0, 
                                                                        instr_name_to_instr_id[as_string(position["symbol"])], 
                                                                        ascii_to_double(position["unrealizedProfit"].get_c_str()), 
                                                                        UNREALIZED_PNL,
                                                                        ACCOUNT_INFO,
                                                                        account_info->exchange_id);
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
                    break;

                case MARGIN_TRANSFER_REQUEST: {
                    MarginTransferRequest *transfer_request = (MarginTransferRequest *) m;
                    MarginTransferResponse transfer_response;
                    std::string post_data;
                    std::stringstream body;
                    std::string body_str;
                    std::string response;
                    std::string asset_name = "";

                    std::string rest_endpoint = listenkey_map[16]->rest_endpoint_margin;

                    if ((transfer_request->from_exchange_id >=16) &&
                        (transfer_request->from_exchange_id <=18))
                    {
                        if (asset_id_to_base_name.count(transfer_request->asset_id)) {
                            asset_name = boost::algorithm::to_upper_copy(asset_id_to_base_name[transfer_request->asset_id]);
                            logger->msg(INFO, "Found matching Asset name for asset id: " + asset_name);
                        } else {
                            logger->msg(INFO, "Found no matching Asset name for asset id: " + std::to_string(transfer_request->asset_id));
                        }
                        // These are common amongst all updates
                        uint8_t from_exchange_id = 0;
                        uint8_t to_exchange_id = 0;
                        uint8_t binance_transfer_type = 0;
                        switch(transfer_request->from_exchange_id){
                            case 16: // Spot account
                                if (transfer_request->to_exchange_id == 18)
                                {
                                    binance_transfer_type = BINANCE_SPOT_TO_USDT;
                                    logger->msg(INFO, "Spot to USDT transfertype detected for Margin transfer on Binance");
                                }
                                else if (transfer_request->to_exchange_id == 17)
                                {
                                    binance_transfer_type = BINANCE_SPOT_TO_COIN;
                                    logger->msg(INFO, "Spot To Coin transfertype detected for Margin transfer on Binance");
                                }
                                else
                                {
                                    binance_transfer_type = UNKNOWN_FUTURES_ACCOUNT;
                                    logger->msg(INFO, "Not able to identify transfertype as wrong to_exchange_id, exiting the request");
                                    return;
                                }
                                break;

                            case 18: // USDT Futures account
                                if (transfer_request->to_exchange_id == 16)
                                {
                                    binance_transfer_type = BINANCE_USDT_TO_SPOT;
                                    logger->msg(INFO, "Spot to USDT transfertype detected for Margin transfer on Binance");
                                }
                                else
                                {
                                    binance_transfer_type = UNKNOWN_FUTURES_ACCOUNT;
                                    logger->msg(INFO, "Not able to identify transfertype as wrong to_exchange_id, exiting the request");
                                    return;
                                }
                                break;

                            case 17: // COIN (DEX) Futures account
                                if (transfer_request->to_exchange_id == 16)
                                {
                                    binance_transfer_type = BINANCE_COIN_TO_SPOT;
                                    logger->msg(INFO, "Spot to USDT transfertype detected for Margin transfer on Binance");
                                }
                                else
                                {
                                    binance_transfer_type = UNKNOWN_FUTURES_ACCOUNT;
                                    logger->msg(INFO, "Not able to identify transfertype as wrong to_exchange_id, exiting the request");
                                    return;
                                }
                                break;
                        }

                        body << "asset=" << asset_name;
                        std::stringstream amount_to_transfer;
                        amount_to_transfer << std::fixed << std::setprecision (8) << transfer_request->transfer_sum;
                        body << "&amount=" << amount_to_transfer.str();
                        body << "&type=" << std::to_string(binance_transfer_type);
                        body << "&timestamp=" << get_current_ts()/1000000; 
                        body_str = body.str();
                        std::string signature = hmacHex(SECRET_KEY, body_str);
                        post_data = "?" + body_str + "&signature=" + signature;
                        std::string tmp_url = rest_endpoint + "transfer" + post_data;
                        logger->msg(INFO, "URL = " + tmp_url);
                        struct curl_slist *chunk = NULL;
                        std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                        chunk = curl_slist_append(chunk, hdrs.c_str());
                        curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
                        CURLcode rc = curl_easy_perform(curl);
                        if (rc) {
                            logger->msg(ERROR, curl_easy_strerror(rc));
                            return;
                        }
                        logger->msg(INFO, "transfer response = " + response);
                        simdjson::dom::element exchange_json_message;
                        auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                        if (error) { 
                            std::stringstream error_message; 
                            error_message << error;
                            logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                            return; 
                        }

                        if( (exchange_json_message["code"].error() != simdjson::NO_SUCH_FIELD) && 
                            (exchange_json_message["msg"].error() != simdjson::NO_SUCH_FIELD)) {
                            RiskRequestReject reject;
                            reject.msg_header = {sizeof(RiskRequestReject), RISK_REQUEST_REJECT, 1};
                            memset(reject.reject_message, 0, 128);
                            strncpy(reject.reject_message, as_string(exchange_json_message["msg"]).c_str(), 127);
                            reject.exchange_id = 16;
                            reject.rejected_id = transfer_request->internal_transfer_id;
                            reject.reject_reason = 0;
                            send_risk_request_reject(&reject);
                            logger->msg(INFO, "Got Exchange Margin Transfer Reject for: " + std::to_string(transfer_request->internal_transfer_id));
                        } 
                        else {
                            transfer_response.msg_header = {sizeof(MarginTransferResponse), MARGIN_TRANSFER_RESPONSE, 0};
                            transfer_response.internal_transfer_id = transfer_request->internal_transfer_id;
                            transfer_response.asset_id = transfer_request->asset_id;
                            transfer_response.from_exchange_id = from_exchange_id;
                            transfer_response.to_exchange_id = to_exchange_id;
                            transfer_response.strategy_id = transfer_request->strategy_id;
                            transfer_response.from_exchange_id = transfer_request->from_exchange_id;
                            transfer_response.to_exchange_id = transfer_request->to_exchange_id;

                            if(exchange_json_message["tranId"].error() != simdjson::NO_SUCH_FIELD) {
                                // got a successful response from exchange
                                transfer_response.transferred_sum = transfer_request->transfer_sum;
                                transfer_response.external_transfer_id = exchange_json_message["tranId"].get_uint64();
                            } else {
                                // somehow a bad response from exchange - go back with 0 amount and 0 transferid
                                transfer_response.transferred_sum = (double) 0.0;
                                transfer_response.external_transfer_id = 0;
                            }
                            send_margin_transfer_response(&transfer_response);
                        }
                    }
                }
                    break;

                case MARGIN_BORROW_REQUEST: {
                    MarginBorrowRequest *borrow_request = (MarginBorrowRequest *) m;
                    std::string post_data;
                    std::stringstream body;
                    std::string body_str;
                    std::string response;
                    std::string asset_name = "";
                    std::string collateral_asset_name = "";
                    std::string rest_endpoint = listenkey_map[16]->rest_endpoint_margin;
                    
                    // Only binance specific requests
                    if((borrow_request->exchange_id < 16) || (borrow_request->exchange_id > 18)){
                        break;
                    }

                    // We only support this for Binance - no other exchanges has this API
                    if (borrow_request->exchange_id != 16) {
                        // Send generic riskrequestreject
                        logger->msg(INFO, "Not supported for this exchange, sending RiskRequestReject back to requestor");
                        RiskRequestReject reject;
                        reject.msg_header = {sizeof(RiskRequestReject), RISK_REQUEST_REJECT, 1};
                        memset(reject.reject_message, 0, 128);
                        strncpy(reject.reject_message, "Not supported for this exchange", 128);
                        reject.exchange_id = borrow_request->exchange_id;
                        reject.rejected_id = borrow_request->internal_borrow_id;
                        reject.reject_reason = 0;
                        send_risk_request_reject(&reject);
                        return;
                    }

                    if (asset_id_to_base_name.count(borrow_request->borrow_asset_id)) {
                        asset_name = boost::algorithm::to_upper_copy(asset_id_to_base_name[borrow_request->borrow_asset_id]);
                        logger->msg(INFO, "Found matching Asset name for asset id: " + asset_name);
                    } else {
                        logger->msg(INFO, "Found no matching Asset name for asset id: " + std::to_string(borrow_request->borrow_asset_id));
                    }
                    if (asset_id_to_base_name.count(borrow_request->collateral_asset_id)) {
                        collateral_asset_name = boost::algorithm::to_upper_copy(asset_id_to_base_name[borrow_request->collateral_asset_id]);
                        logger->msg(INFO, "Found matching Asset name for asset id: " + asset_name);
                    } else {
                        logger->msg(INFO, "Found no matching Asset name for asset id: " + std::to_string(borrow_request->collateral_asset_id));
                    }

                    body << "coin=" << asset_name;
                    std::stringstream amount_to_borrow;
                    amount_to_borrow << std::fixed << std::setprecision (8) << borrow_request->borrow_amount;
                    body << "&amount=" << amount_to_borrow.str();
                    body << "&collateralCoin=" << collateral_asset_name;
                    std::stringstream collateral_amount;
                    collateral_amount << std::fixed << std::setprecision (8) << borrow_request->collateral_amount;
                    body << "&collateralAmount=" << collateral_amount.str();
                    body << "&timestamp=" << get_current_ts()/1000000; 
                    body_str = body.str();
                    std::string signature = hmacHex(SECRET_KEY, body_str);
                    post_data = "?" + body_str + "&signature=" + signature;
                    std::string tmp_url = rest_endpoint + "loan/borrow" + post_data;
                    logger->msg(INFO, "URL = " + tmp_url);
                    struct curl_slist *chunk = NULL;
                    std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                    chunk = curl_slist_append(chunk, hdrs.c_str());
                    curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
                    CURLcode rc = curl_easy_perform(curl);
                    if (rc) {
                        logger->msg(ERROR, curl_easy_strerror(rc));
                        return;
                    }
                    logger->msg(INFO, "margin borrow response=" + response);
                    simdjson::dom::element exchange_json_message;
                    auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                    if (error) { 
                        std::stringstream error_message; 
                        error_message << error;
                        logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                        return; 
                    }

                    if( (exchange_json_message["code"].error() != simdjson::NO_SUCH_FIELD) && 
                        (exchange_json_message["msg"].error() != simdjson::NO_SUCH_FIELD)) {
                        RiskRequestReject reject;
                        reject.msg_header = {sizeof(RiskRequestReject), RISK_REQUEST_REJECT, 1};
                        memset(reject.reject_message, 0, 128);
                        strncpy(reject.reject_message, as_string(exchange_json_message["msg"]).c_str(), 127);
                        reject.exchange_id = 16;
                        reject.rejected_id = borrow_request->internal_borrow_id;
                        reject.reject_reason = 0;
                        send_risk_request_reject(&reject);
                        logger->msg(INFO, "Got Exchange Margin Borrow Request Reject for: " + std::to_string(borrow_request->internal_borrow_id));
                    } 
                    else {

                        MarginBorrowResponse margin_response;
                        // These are common amongst all updates
                        margin_response.msg_header = {sizeof(MarginBorrowResponse), MARGIN_BORROW_RESPONSE, 0};
                        margin_response.internal_borrow_id = borrow_request->internal_borrow_id;
                        margin_response.strategy_id = borrow_request->strategy_id;
                        margin_response.exchange_id = borrow_request->exchange_id;

                        if(exchange_json_message["coin"].error() != simdjson::NO_SUCH_FIELD) {
                            // got a successful response from exchange
                            margin_response.sum_borrowed = ascii_to_double(exchange_json_message["amount"].get_c_str());
                            margin_response.external_borrow_id = ascii_to_double(exchange_json_message["borrowId"].get_c_str());
                        } else {
                            // somehow a bad response from exchange - go back with 0 amount and 0 borrowid
                            margin_response.sum_borrowed = (double) 0.0;
                            margin_response.external_borrow_id = 0;
                        }
                        send_margin_borrow_response(&margin_response);
                    }
                }
                    break;

                case MARGIN_INFO_REQUEST: {
                    MarginInfoRequest *info_request = (MarginInfoRequest *) m;
                    MarginInfoResponse margin_response;
                    std::string post_data;
                    std::stringstream body;
                    std::string body_str;
                    std::string response;
                    std::string rest_endpoint = listenkey_map[16]->rest_endpoint_margin_v2;

                    // Only binance specific requests
                    if((info_request->exchange_id < 16) || (info_request->exchange_id > 18)){
                        break;
                    }

                    // We only support this for Binance - no other exchanges has this API
                    if (info_request->exchange_id != 16) {
                        // Send generic riskrequestreject
                        logger->msg(INFO, "Not supported for this exchange, sending RiskRequestReject back to requestor");
                        RiskRequestReject reject;
                        reject.msg_header = {sizeof(RiskRequestReject), RISK_REQUEST_REJECT, 1};
                        memset(reject.reject_message, 0, 128);
                        strncpy(reject.reject_message, "Not supported for this exchange", 128);
                        reject.exchange_id = info_request->exchange_id;
                        reject.rejected_id = 0;
                        reject.reject_reason = 0;
                        send_risk_request_reject(&reject);
                        return;
                    }

                    switch(info_request->info_type_reason) {
                        case CROSS_COLLATERAL_WALLET: {
                            body << "timestamp=" << get_current_ts()/1000000; 
                            body_str = body.str();
                            std::string signature = hmacHex(SECRET_KEY, body_str);
                            post_data = "?" + body_str + "&signature=" + signature;
                            std::string tmp_url = rest_endpoint + "loan/wallet" + post_data;
                            logger->msg(INFO, "USL = " + tmp_url);
                            struct curl_slist *chunk = NULL;
                            std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                            chunk = curl_slist_append(chunk, hdrs.c_str());
                            curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
                            CURLcode rc = curl_easy_perform(curl);
                            if (rc) {
                                logger->msg(ERROR, curl_easy_strerror(rc));
                                return;
                            }
                            logger->msg(INFO, "Cross Collateral Wallet response=" + response);
                            simdjson::dom::element exchange_json_message;
                            auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                            if (error) { 
                                std::stringstream error_message; 
                                error_message << error;
                                logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                                return; 
                            }

                            if(exchange_json_message["totalCrossCollateral"].error() != simdjson::NO_SUCH_FIELD) {
                                // These are common amongst all updates
                                margin_response.msg_header = {sizeof(MarginInfoResponse), MARGIN_INFO_RESPONSE, 0};
                                margin_response.total_cross_collateral = ascii_to_double(exchange_json_message["totalCrossCollateral"].get_c_str());
                                margin_response.total_borrowed = ascii_to_double(exchange_json_message["totalBorrowed"].get_c_str());

                                // Process per loancoin update
                                for (simdjson::dom::element margin_element : exchange_json_message["crossCollaterals"]) {
                                    std::string asset_loancoin = boost::algorithm::to_lower_copy(as_string(margin_element["loanCoin"]));
                                    std::string asset_collateralcoin = boost::algorithm::to_lower_copy(as_string(margin_element["collateralCoin"]));
                                    logger->msg(INFO, "Asset Loancoin: " + asset_loancoin);
                                    if (base_name_to_asset_id.count(asset_loancoin)) {
                                        margin_response.loan_asset_id = base_name_to_asset_id[asset_loancoin];
                                        logger->msg(INFO, "Matched with Asset ID for loancoin: " + std::to_string(margin_response.loan_asset_id));
                                    } else {
                                        logger->msg(INFO, "Not able to find a asset id for the loancoin");
                                        margin_response.loan_asset_id = 0;
                                    }
                                    logger->msg(INFO, "Asset Collateralcoin: " + asset_collateralcoin);
                                    if (base_name_to_asset_id.count(asset_collateralcoin)) {
                                        margin_response.collateral_asset_id = base_name_to_asset_id[asset_collateralcoin];
                                        logger->msg(INFO, "Matched with Asset ID for collateralcoin: " + std::to_string(margin_response.collateral_asset_id));
                                    } else {
                                        logger->msg(INFO, "Not able to find a asset id for the collateralcoin");
                                        margin_response.collateral_asset_id = 0;
                                    }

                                    margin_response.locked_amount = ascii_to_double(margin_element["locked"].get_c_str());
                                    margin_response.loan_amount = ascii_to_double(margin_element["loanAmount"].get_c_str());
                                    margin_response.current_collateral_rate = ascii_to_double(margin_element["currentCollateralRate"].get_c_str());
                                    margin_response.interest = ascii_to_double(margin_element["interest"].get_c_str());
                                    send_margin_info_response(&margin_response);
                                }
                            }
                        }
                        break;

                        case CROSS_COLLATERAL_INFO: {
                            logger->msg(INFO, "Retrieved new margin info request for Cross Collateral Info from ringbuffer");
                            if(info_request->collateral_asset_id != 0) {
                                logger->msg(INFO, "Collateral Info for: " + std::to_string(info_request->collateral_asset_id) + " requested");
                                body << "collateralCoin=" << asset_id_to_base_name[info_request->collateral_asset_id];
                            }
                            body << "timestamp=" << get_current_ts()/1000000; 
                            body_str = body.str();
                            std::string signature = hmacHex(SECRET_KEY, body_str);
                            post_data = "?" + body_str + "&signature=" + signature;
                            std::string tmp_url = rest_endpoint + "loan/configs" + post_data;
                            logger->msg(INFO, "rest_url = " + tmp_url);
                            struct curl_slist *chunk = NULL;
                            std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
                            chunk = curl_slist_append(chunk, hdrs.c_str());
                            curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
                            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
                            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
                            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
                            CURLcode rc = curl_easy_perform(curl);
                            if (rc) {
                                logger->msg(ERROR, curl_easy_strerror(rc));
                                return;
                            }
                            logger->msg(INFO, "Cross Collateral Info response from exchange: " + response);
                            simdjson::dom::element exchange_json_message;
                            auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
                            if (error) { 
                                std::stringstream error_message; 
                                error_message << error;
                                logger->msg(ERROR, "Got an error when parsing: " + error_message.str());
                                return; 
                            }

                            margin_response.msg_header = {sizeof(MarginInfoResponse), MARGIN_INFO_RESPONSE, 0};
                            for (simdjson::dom::element margin_element : exchange_json_message) {
                                if (margin_element["collateralCoin"].error() != simdjson::NO_SUCH_FIELD){
                                    std::string asset_lc = boost::algorithm::to_lower_copy(as_string(margin_element["collateralCoin"]));
                                    logger->msg(INFO, "Asset Collateralcoin: " + asset_lc);
                                    if (base_name_to_asset_id.count(asset_lc)) {
                                        margin_response.collateral_asset_id = base_name_to_asset_id[asset_lc];
                                        logger->msg(INFO, "Matched with Asset ID for collateralcoin: " + std::to_string(margin_response.collateral_asset_id));
                                    } else {
                                        logger->msg(INFO, "Not able to find a asset id for the collateralcoin");
                                        margin_response.collateral_asset_id = 0;
                                    }
                                    margin_response.current_collateral_rate = ascii_to_double(margin_element["currentCollateralRate"].get_c_str());
                                    margin_response.liquidation_collateral_rate = ascii_to_double(margin_element["liquidationCollateralRate"].get_c_str());
                                    margin_response.margin_call_collateral_rate = ascii_to_double(margin_element["marginCallCollateralRate"].get_c_str());
                                    margin_response.rate = ascii_to_double(margin_element["rate"].get_c_str());
                                    margin_response.interest = ascii_to_double(margin_element["interestRate"].get_c_str());
                                    send_margin_info_response(&margin_response);
                                }
                            }
                        }
                        break;
                    }
                }
                    break;

                }
    };
}

// =================================================================================
// Load all open orders into datastructures so that we can cancel etc
// =================================================================================
void BinanceTradeAdapter::load_all_open_orders(Logger *logger, uint8_t exchange_id) {
    CURL *curl;
    std::string post_data;
    std::stringstream body;
    std::string body_str;
    std::string response;
    body << "recvWindow=5000&timestamp=" << get_current_ts_millis(); 
    body_str = body.str();
    std::string signature = hmacHex(SECRET_KEY, body_str);
    post_data = "?" + body_str + "&signature=" + signature;
    std::string tmp_url = listenkey_map[exchange_id]->rest_endpoint + "openOrders" + post_data;
    logger->msg(INFO, "URL: " + tmp_url);
    struct curl_slist *chunk = NULL;
    std::string hdrs = "X-MBX-APIKEY: " + API_KEY;
    curl = curl_easy_init();
    chunk = curl_slist_append(chunk, hdrs.c_str());
    curl_easy_setopt(curl, CURLOPT_URL, tmp_url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_func);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");
    CURLcode rc = curl_easy_perform(curl);
    if (rc) {
        logger->msg(ERROR, curl_easy_strerror(rc));
        return;
    }

    logger->msg(INFO, "openOrder request response=" + response);

    simdjson::dom::element exchange_json_message;
    auto error = parser.parse(response.c_str(), response.length()).get(exchange_json_message);
    if (error) { 
        std::stringstream error_message; 
        error_message << error;
        logger->msg(ERROR, "Got an error when parsing openOrders request: " + error_message.str());
    }

    for (simdjson::dom::element order_element : exchange_json_message) {
        if (instr_name_to_instr_id.count(as_string(order_element["symbol"]))) {
            uint32_t instrument_id = instr_name_to_instr_id[as_string(order_element["symbol"])];
            std::string client_order_id = as_string(order_element["clientOrderId"]);
            // double order_price = ascii_to_double(order_element["price"].get_c_str());
            // double order_quantity = ascii_to_double(order_element["origQty"].get_c_str());
            // double executed_quantity = ascii_to_double(order_element["executedQty"].get_c_str());
            logger->msg(INFO, "Found open order for: " + as_string(order_element["symbol"]) + " (instrument_id: " + std::to_string(instrument_id) + ")");
        }
    }


}

// =================================================================================
// CONSTRUCTOR (the only one..:)
// =================================================================================
BinanceTradeAdapter::BinanceTradeAdapter(   std::string location, 
                                            uint8_t order_environment) : BaseTradeAdapter(order_environment, "Binance_All_Exchanges") 
{

    // This pulls the key values into the ConfigDB object from the config database
    // Can later be fetched by get_config_value("keyname");
    load_config("svc_oe_binance");

    // Initialise the user_websocket threads with appropriate loggers
    user_websockets = new WSock(log_worker->get_new_logger("user_websockets_main"), log_worker->get_new_logger("user_websockets_subscriptions"), 36000, 50);

    set_exchange_state(_NOT_CONNECTED);

    API_KEY = get_config_value("API_KEY", "ALL");
    SECRET_KEY = get_config_value("SECRET_KEY", "ALL");
    
    std::string hdrs = "X-MBX-APIKEY: " + API_KEY;

    curl = curl_easy_init();

    new_order_curl = curl_easy_init();
    curl_easy_setopt(new_order_curl, CURLOPT_POST, 1L);
    curl_easy_setopt(new_order_curl, CURLOPT_WRITEFUNCTION, curl_write_func);
    new_order_chunk = curl_slist_append(new_order_chunk, hdrs.c_str());
    curl_easy_setopt(new_order_curl, CURLOPT_WRITEDATA, &new_order_response);
    curl_easy_setopt(new_order_curl, CURLOPT_HTTPHEADER, new_order_chunk);
    curl_easy_setopt(new_order_curl, CURLOPT_POSTFIELDS, "");
    curl_easy_setopt(new_order_curl, CURLOPT_CUSTOMREQUEST, "POST");

    cancel_curl = curl_easy_init();
    curl_easy_setopt(cancel_curl, CURLOPT_WRITEFUNCTION, curl_write_func);
    cancel_chunk = curl_slist_append(cancel_chunk, hdrs.c_str());
    curl_easy_setopt(cancel_curl, CURLOPT_WRITEDATA, &cancel_response);
    curl_easy_setopt(cancel_curl, CURLOPT_HTTPHEADER, cancel_chunk);
    curl_easy_setopt(cancel_curl, CURLOPT_POSTFIELDS, "");
    curl_easy_setopt(cancel_curl, CURLOPT_CUSTOMREQUEST, "DELETE");

    fragment_handler = std::bind(&BinanceTradeAdapter::aeron_msg_handler, this);
    from_aeron_io = new from_aeron(AERON_IO, fragment_handler);    

    std::string rest_endpoint;
    std::list<std::string> exchange_list {"Binance", "Binance Futures", "BinanceDEX"};
    
    // Adding Binance
    listenkey_map[16] = new user_websocket_info();
    listenkey_map[16]->rest_endpoint = get_config_value("rest_endpoint", "Binance");
    listenkey_map[16]->ws_endpoint = get_config_value("ws_endpoint", "Binance");
    listenkey_map[16]->rest_endpoint_margin = get_config_value("rest_endpoint_margin", "Binance");
    listenkey_map[16]->rest_endpoint_margin_v2 = get_config_value("rest_endpoint_margin_v2", "Binance");
    listenkey_map[16]->exchange_name = "Binance";
    add_user_websocket(logger, 16, "Binance");
    load_all_open_orders(log_worker->get_new_logger("open_order_loader"), 16);


    // Adding Binance Futures
    listenkey_map[18] = new user_websocket_info();
    listenkey_map[18]->rest_endpoint = get_config_value("rest_endpoint", "Binance Futures");
    listenkey_map[18]->ws_endpoint = get_config_value("ws_endpoint", "Binance Futures");
    listenkey_map[18]->exchange_name = "Binance Futures";
    add_user_websocket(logger, 18, "Binance Futures");
    load_all_open_orders(log_worker->get_new_logger("open_order_loader"), 18);

    
    // Adding BinanceDEX
    listenkey_map[17] = new user_websocket_info();
    listenkey_map[17]->rest_endpoint = get_config_value("rest_endpoint", "BinanceDEX");
    listenkey_map[17]->ws_endpoint = get_config_value("ws_endpoint", "BinanceDEX");
    listenkey_map[17]->exchange_name = "BinanceDEX";
    add_user_websocket(logger, 17, "BinanceDEX");
    load_all_open_orders(log_worker->get_new_logger("open_order_loader"), 17);


    // Loop around the websockets
    user_websocket_loop_thread(log_worker->get_new_logger("websocket_messageloop_thread"));

    // This loops across the listenkeys and ensures they are renewed
    listenkey_refresh_thread(log_worker->get_new_logger("listenkey_refresh_thread"));
}