#include "base_trade_adapter.hpp"


// =================================================================================
BaseTradeAdapter::BaseTradeAdapter(uint8_t order_environment, std::string exchange_name) {
    _order_environment = order_environment;

    // Initialise callback function pointers
    std::function<void(struct InstrumentInfoResponse *)> add_instr_func = 
                std::bind(&BaseTradeAdapter::update_instrument_info, this, std::placeholders::_1);
    std::function<void(struct StrategyInfoResponse *)> add_strategy_func =  
                std::bind(&BaseTradeAdapter::update_strategy_info, this, std::placeholders::_1);

    // Initialise the configuration
    if(_order_environment == UAT_ENV) {
        log_worker = new LogWorker("trade_adapter", exchange_name, "UAT");
        config_items = new ConfigDB("UAT", log_worker->get_new_logger("configdb"));
    } 
    else if(_order_environment == PRODUCTION_ENV) {
        log_worker = new LogWorker("trade_adapter", exchange_name, "PROD");
        config_items = new ConfigDB("PROD", log_worker->get_new_logger("configdb"));
    }
    logger = log_worker->get_new_logger("base");

    oe_unique_order_id = generate_hash_for_oe(unique_part_for_order_id);

    // wait for risk to be UP 
    // logger->msg(INFO, "Waiting for heartbeat request on risk channel before continuing");
    // while(!got_hb) {
    //     sleep(1);
    // }
    // logger->msg(INFO, "Found heartbeat request - continuing");

    
    // Initialise the washbook order id epoch time in milliseconds
    wash_book_order_id = (uint32_t) (get_current_ts() / (uint64_t) 1000000000);
    logger->msg(INFO, "Initialised the washbook orderid to be: " + std::to_string(wash_book_order_id));
    to_aeron_io = new to_aeron(AERON_IO);

    // Ask for all active instruments
    fetch_instruments();
    // Ask Risk Service for all active strategies
    fetch_strategies();

}

// =================================================================================
void BaseTradeAdapter::fetch_instruments() {
    InstrumentInfoRequest info_request = {MessageHeaderT{sizeof(InstrumentInfoRequest), INSTRUMENT_INFO_REQUEST, 0}, 16};
    to_aeron_io->send_data((char *)&info_request, sizeof(InstrumentInfoRequest)); 
    info_request.exchange_id=17;
    to_aeron_io->send_data((char *)&info_request, sizeof(InstrumentInfoRequest)); 
    info_request.exchange_id=18;
    to_aeron_io->send_data((char *)&info_request, sizeof(InstrumentInfoRequest)); 
}

// =================================================================================
void BaseTradeAdapter::fetch_strategies() {
    StrategyInfoRequest strategy_request = {MessageHeaderT{sizeof(StrategyInfoRequest), STRATEGY_INFO_REQUEST, 0}, 16};
    to_aeron_io->send_data((char *)&strategy_request, sizeof(StrategyInfoRequest));
    strategy_request.exchange_id = 17;
    to_aeron_io->send_data((char *)&strategy_request, sizeof(StrategyInfoRequest));
    strategy_request.exchange_id = 18;
    to_aeron_io->send_data((char *)&strategy_request, sizeof(StrategyInfoRequest));    
}

// =================================================================================
void BaseTradeAdapter::send_account_update(uint32_t asset_id, uint32_t instrument_id, double value, uint32_t account_info_type, uint8_t update_reason, uint8_t exchange_id) {
    AccountInfoResponse a;
    a.msg_header = MessageHeaderT{sizeof(AccountInfoResponse), ACCOUNT_INFO_UPDATE, 0};
    a.exchange_id       = exchange_id;
    a.instrument_id     = instrument_id;
    a.value             = value;
    a.asset_id          = asset_id;
    a.account_info_type = account_info_type;  
    a.account_update_reason = update_reason;  
    to_aeron_io->send_data((char*)&a, sizeof(AccountInfoResponse));
}

// =================================================================================
void BaseTradeAdapter::send_margin_alert(AlertMarginCall *alert) {
    AlertMarginCall _alert;
    _alert = *alert;
    to_aeron_io->send_data((char*)&_alert, sizeof(AlertMarginCall));
}

// =================================================================================
void BaseTradeAdapter::send_margin_info_response(MarginInfoResponse *margin_response) {
    to_aeron_io->send_data((char*)margin_response, sizeof(MarginInfoResponse));
}

// =================================================================================
void BaseTradeAdapter::send_margin_borrow_response(MarginBorrowResponse *margin_response) {
    to_aeron_io->send_data((char*)margin_response, sizeof(MarginBorrowResponse));
}

// =================================================================================
void BaseTradeAdapter::send_margin_transfer_response(MarginTransferResponse *margin_response) {
    to_aeron_io->send_data((char*)margin_response, sizeof(MarginTransferResponse));
}

// =================================================================================
void BaseTradeAdapter::send_risk_request_reject(RiskRequestReject *reject) {
    to_aeron_io->send_data((char*)reject, sizeof(RiskRequestReject));
}

// =================================================================================
void BaseTradeAdapter::send_any_io_message(char *message_ptr, int length) {
    MyGuard guard(lock);
    to_aeron_io->send_data(message_ptr, length);
}

// =================================================================================
void BaseTradeAdapter::set_exchange_state(trade_adapter_e new_state) {
    _state.store(new_state, std::memory_order_release);
}

// =================================================================================
trade_adapter_e BaseTradeAdapter::get_exchange_state() {
    return(_state.load(std::memory_order_consume));
}

// =================================================================================
uint8_t BaseTradeAdapter::get_order_env() {
    return(_order_environment);
}

// =================================================================================
std::string BaseTradeAdapter::generate_hash_for_oe(int unique_part_for_order_id) 
{
    static auto& chrs = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

    std::string s;

    s.reserve(unique_part_for_order_id);

    while(unique_part_for_order_id--)
        s += chrs[pick(rg)];
    return s;
}

// =================================================================================
std::string BaseTradeAdapter::as_string(std::string_view v) { 
    return {v.data(), v.size()}; 
}

// =================================================================================
size_t BaseTradeAdapter::curl_write_func(char *buffer, size_t size, size_t nmemb, void *data) {
  std::string *s = (std::string *)data;
  s->append(buffer, size*nmemb);
  return size*nmemb;
}

// =================================================================================
int BaseTradeAdapter::convert_to_hex(char *buffer, unsigned char *array_to_convert, int length_to_convert) {
    int buffer_offset = 0;
    unsigned char char_to_print;
    char lookup_table[] = "0123456789abcdef";

    for(int i=0; i<length_to_convert; i++){
        char_to_print = array_to_convert[i];
        char_to_print >>= 4;
        buffer[buffer_offset++] = lookup_table[char_to_print];
        char_to_print = array_to_convert[i];
        char_to_print <<= 4;
        char_to_print >>= 4;
        buffer[buffer_offset++] = lookup_table[char_to_print];
    }
    return(buffer_offset);
}

// // =================================================================================
// std::string BaseTradeAdapter::hmacHex(std::string key, std::string msg) {  
//     unsigned char hash[32];
//     char buffer[65];
//     buffer[64] = 0;
//     HMAC_CTX *hmac;
//     unsigned int len = 32;

//     hmac = HMAC_CTX_new();
//     HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), NULL);
//     HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
//     HMAC_Final(hmac, hash, &len);
//     HMAC_CTX_free(hmac);
//     convert_to_hex(buffer, hash, 32);
//     return (std::string(buffer, 64));
// }

// =================================================================================
std::string BaseTradeAdapter::hmacHex(std::string key, std::string msg) {  
    Hmac        hmac;
    byte        hmacDigest[SHA256_DIGEST_SIZE];
    char        buffer[65];
    
    buffer[64] = 0;    
    wc_HmacSetKey(&hmac, SHA256, (byte *) key.c_str(), sizeof(key));
    wc_HmacUpdate(&hmac, (byte *) msg.c_str(), sizeof(msg));
    wc_HmacFinal(&hmac, hmacDigest);
    convert_to_hex(buffer, hmacDigest, 32);
    return (std::string(buffer, 64));
}

// // =================================================================================
// int BaseTradeAdapter::hmacHex(std::string key, std::string msg, char *output_string) {  
//     unsigned char hash[32];
//     output_string[64] = 0;
//     HMAC_CTX *hmac;
//     unsigned int len = 32;

//     memcpy(output_string, "&signature=", 11);
//     output_string+=11;

//     hmac = HMAC_CTX_new();
//     HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), NULL);
//     HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
//     HMAC_Final(hmac, hash, &len);
//     HMAC_CTX_free(hmac);
//     convert_to_hex(output_string, hash, 32);
//     return (75);
// }

// =================================================================================
int BaseTradeAdapter::hmacHex(std::string key, std::string msg, char *output_string) {  
    Hmac        hmac;
    byte        hmacDigest[SHA256_DIGEST_SIZE];
    
    output_string[64] = 0;   
    wc_HmacSetKey(&hmac, SHA256, (byte *) key.c_str(), sizeof(key));
    wc_HmacUpdate(&hmac, (byte *) msg.c_str(), sizeof(msg));
    wc_HmacFinal(&hmac, hmacDigest);
    convert_to_hex(output_string, hmacDigest, 32);
    return (75);
}

// =================================================================================
uint64_t BaseTradeAdapter::get_current_ts() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return((t.tv_sec*1000000000L)+t.tv_nsec);
}

// =================================================================================
uint64_t BaseTradeAdapter::get_current_ts_millis() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    return((time_now.tv_sec * 1000) + (time_now.tv_usec / 1000));
}

// =================================================================================
bool BaseTradeAdapter::load_config(std::string app_name) {
    logger->msg(INFO, "Loading configuration from DB");
    config_items->pull_config_for_app_instance(app_name);
    return(true);
}

// =================================================================================
std::string BaseTradeAdapter::get_config_value(std::string key, std::string instance_name) {
    return(config_items->get_value_for_key(key, instance_name));
}

// =================================================================================
void BaseTradeAdapter::update_instrument_info(struct InstrumentInfoResponse *instrument_info) { 
    if((instrument_info->exchange_id >= 16) && (instrument_info->exchange_id <= 18)) {
        if(! instrument_info_map.count(instrument_info->instrument_id)) {
            // Deep copy of the struct char elements
            struct InstrumentInfoResponse *new_instrument_info = new InstrumentInfoResponse();
            *new_instrument_info = *instrument_info;
            strncpy(new_instrument_info->instrument_name, instrument_info->instrument_name, 20);
            strncpy(new_instrument_info->exchange_name, instrument_info->exchange_name, 20);
            strncpy(new_instrument_info->base_asset_code, instrument_info->base_asset_code, 20);
            strncpy(new_instrument_info->quote_asset_code, instrument_info->quote_asset_code, 20);
            instrument_info_map[instrument_info->instrument_id] = new_instrument_info;

            // These are used for convenience later
            std::string instrument_name(instrument_info->instrument_name);
            std::string base_asset_code(instrument_info->base_asset_code);
            std::string quote_asset_code(instrument_info->quote_asset_code);

            instr_id_to_instr_name[instrument_info->instrument_id] = instrument_name;
            instr_name_to_instr_id[instrument_name] = instrument_info->instrument_id;
            base_name_to_asset_id[base_asset_code] = instrument_info->base_asset_id;
            base_name_to_asset_id[quote_asset_code] = instrument_info->quote_asset_id;
            asset_id_to_base_name[instrument_info->base_asset_id] = base_asset_code;
            asset_id_to_base_name[instrument_info->quote_asset_id] = quote_asset_code;
        }

        // } else {
        //     if(instrument_info->is_live < 2) {
        //         // Check if ours is live..
        //         if (! instrument_info_map[instrument_info->instrument_id]->is_live) {
        //             // Ours is not live, but the one we received is, we need to update ours and report
        //             instrument_info_map[instrument_info->instrument_id]->is_live = true;
        //             std::string instrument_name(instrument_info->instrument_name);
        //             logger->msg(INFO, "Instrument set to live from dead: " + instrument_name);
        //         }
        //     }
        // }
    }
}

// =================================================================================
void BaseTradeAdapter::update_strategy_info(struct StrategyInfoResponse *strategy_info) {
    if(! strategy_info_map.count(strategy_info->strategy_id)){
        // This is a new strategy - add it
        auto new_strategy = new StrategyInfoResponse();
        *new_strategy = *strategy_info;
        strategy_info_map[strategy_info->strategy_id] = new_strategy;
    } else {
        // We need to update the riskcheck values to the already existing strategy
        auto strategy = strategy_info_map[strategy_info->strategy_id];
        strategy->max_order_value_USD = strategy_info->max_order_value_USD;
        strategy->max_tradeconsideration_USD = strategy_info->max_tradeconsideration_USD;
    }
}

// =================================================================================
bool BaseTradeAdapter::order_pass_riskcheck(struct SendOrder *order_to_send, std::string &reject_message, uint8_t &reject_reason) {
    bool order_ok = true;
    // memset(request_ack->reject_message, 0, MAX_REJECT_LENGTH);

    // Extracing order_environment
    uint64_t order_order_env = order_to_send->internal_order_id>>56;
    double max_order_value = 1000.00;
    double contract_size = 0.000000;
    if(strategy_info_map.count(order_to_send->strategy_id))
        max_order_value = strategy_info_map[order_to_send->strategy_id]->max_order_value_USD;
    
    if(instrument_info_map.count(order_to_send->instrument_id))
        contract_size = instrument_info_map[order_to_send->instrument_id]->contract_size;

    // Order environment of order is the same setting of exchange config
    if (order_order_env != _order_environment){
        logger->msg(INFO, "failed riskchecks for order_environment");
        std::string reject_additional_message = "";
        if (_order_environment == UAT_ENV) {
            if (order_order_env == UNKNOWN_ENV) {
                reject_additional_message = "ENV IS UAT, ORDER IS UNKOWN";
            } else if (order_order_env == PRODUCTION_ENV) {
                reject_additional_message = "ENV IS UAT, ORDER IS PRODUCTION";
            }
        } else {
            if (order_order_env == UNKNOWN_ENV) {
                reject_additional_message = "ENV IS PRODUCTION, ORDER IS UNKOWN";
            } else if (order_order_env == PRODUCTION_ENV) {
                reject_additional_message = "ENV IS PRODUCTION, ORDER IS UAT";
            }
        }
        reject_message += "WRONG ORDER_ENV: ";
        reject_message += reject_additional_message;
        reject_reason = ORDER_FOR_WRONG_ENVIRONMENT;
        order_ok = false; 
    }

    // No risk value!
    // else if (! strategy_info_map.count(order_to_send->strategy_id)) {
    //     logger->msg(INFO, "failed riskchecks for no strategy risk information");
    //     reject_reason = RISK_REJECT;
    //     reject_message = "CANNOT FIND RISK VALUE";
    //     order_ok = false;
    // }

    // Breach MAX Order value contract_size
    else if( (contract_size > 0.000000) && ((order_to_send->qty * contract_size) > max_order_value)) {
        logger->msg(INFO, "failed riskchecks for max ordervalue - calculated against contract size");
        double ord_val = order_to_send->qty * contract_size;
        reject_message = "BREACH MAX ORDER VALUE: ";
        reject_message += std::to_string(ord_val);
        reject_reason = RISK_REJECT;
        order_ok = false;
    }

    // Breach MAX Order value normal price
    else if ((contract_size < 0.000001) && ((order_to_send->qty * order_to_send->price) > max_order_value)) {
        logger->msg(INFO, "failed riskchecks for max ordervalue");
        double ord_val = order_to_send->qty * order_to_send->price;
        reject_message = "BREACH MAX ORDER VALUE: ";
        reject_message += std::to_string(ord_val);
        reject_reason = RISK_REJECT;
        order_ok = false;
    }

    // Instrument is not set to live
    // else if (! instrument_info_map[order_to_send->instrument_id]->is_live) {
    //     logger->msg(INFO, "failed riskchecks for instrument not live");
    //     reject_message = "INSTRUMENT NOT LIVE, CANNOT SEND ORDER: " + std::to_string(order_to_send->instrument_id);
    //     reject_reason = RISK_REJECT;
    //     order_ok = false;
    // }
    return(order_ok);
}

// =================================================================================
uint64_t BaseTradeAdapter::ascii_to_lserep( const char * str )
{
    uint64_t final_val = 0;
    uint64_t decimal_val = 0;


    while( *str  && (*str != '.')) {
        final_val = final_val*10 + (*str++ - '0');
    }
    final_val *= 100000000;
    
    if (*str == '.') {
        str++;
        uint64_t multiplier = 10000000;

        while( *str) {
            decimal_val += (*str++ - '0') * multiplier;
            multiplier /= 10;
        }    
    }

    return (final_val + decimal_val);
}

// =================================================================================
double BaseTradeAdapter::ascii_to_double( const char * str )
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

// =================================================================================
void BaseTradeAdapter::uint_to_ascii(unsigned int val, char* c)
{
    static const char digitPairs[201] = {
        "00010203040506070809"
        "10111213141516171819"
        "20212223242526272829"
        "30313233343536373839"
        "40414243444546474849"
        "50515253545556575859"
        "60616263646566676869"
        "70717273747576777879"
        "80818283848586878889"
        "90919293949596979899"
    };

    int size;

    if(val >= 10000) {
        if(val >= 10000000) {
            if(val >= 1000000000)
                size = 10;
            else if(val >= 100000000)
                size = 9;
            else
                size = 8;
        } else {
            if(val >= 1000000)
                size = 7;
            else if(val >= 100000)
                size = 6;
            else
                size = 5;
        }
    } else {
        if(val >= 100) {
            if(val >= 1000)
                size = 4;
            else
                size = 3;
        } else {
            if(val >= 10)
                size = 2;
            else
                size = 1;
        }
    }

    c += size - 1;
    c[1] = 0;

    while(val >= 100) {
        int pos = (val % 100) * 2;
        val /= 100;
        c[-1] = digitPairs[pos];
        c[0] = digitPairs[pos + 1];
        c -= 2;
    }

    if (val < 10)
        c[0] = '0' + val;
    else {
        c[-1] = digitPairs[val * 2];
        c[0] = digitPairs[val * 2 + 1];
    }
}

// =================================================================================
uint64_t BaseTradeAdapter::get_next_washbook_internal_order_id(std::string external_order_id, uint8_t exchange_id) {
    uint64_t temp_location_id = 1;
    uint64_t tmep_environment_id = get_order_env();
    uint64_t temp_exchange_id = exchange_id;
    tmep_environment_id <<= 56;
    temp_location_id <<= 48;
    temp_exchange_id <<= 40;
    wash_book_order_id++;
    uint64_t new_order_id = wash_book_order_id + tmep_environment_id + temp_location_id + temp_exchange_id;
    washbook_external_to_internal_order_id[external_order_id] = new_order_id;
    return(new_order_id);
}

// =================================================================================
uint64_t BaseTradeAdapter::get_internalid_from_external_map(std::string external_id) {
    if(washbook_external_to_internal_order_id.count(external_id)) {
        return(washbook_external_to_internal_order_id[external_id]);
    } else {
        return (0);
    }
}

// =================================================================================
void BaseTradeAdapter::send_internal_order_ack(char *external_order_id, struct SendOrder *send_order) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 1};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.exchange_id = send_order->exchange_id;
    r.ack_type = REQUEST_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);

    // These need to be supplied by the arguments
    r.internal_order_id = send_order->internal_order_id;
    r.instrument_id = send_order->instrument_id;
    r.strategy_id = send_order->strategy_id;
    r.send_timestamp = get_current_ts();       
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_internal_order_reject(struct SendOrder *send_order, std::string reject_message, uint8_t reject_reason) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 1};
    memset(r.external_order_id, 0, MAX_EXTERNAL_ORDER_ID_LENGTH);
    r.exchange_id = send_order->exchange_id;
    r.ack_type = REQUEST_REJECT;
    r.reject_reason = reject_reason;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);
    strncpy(r.reject_message, reject_message.c_str(), MAX_REJECT_LENGTH-1);

    // These need to be supplied by the arguments
    r.internal_order_id = send_order->internal_order_id;
    r.instrument_id = send_order->instrument_id;
    r.strategy_id = send_order->strategy_id;
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_internal_cancel_ack(struct CancelOrder *cancel_request, char *external_order_id) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 1};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.exchange_id = cancel_request->exchange_id;
    r.ack_type = CANCEL_REQUEST_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);

    // These need to be supplied by the arguments
    r.internal_order_id = cancel_request->internal_order_id;
    r.instrument_id = cancel_request->instrument_id;
    r.strategy_id = cancel_request->strategy_id;
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_internal_cancel_reject(struct CancelOrder *cancel_request, std::string reject_message, uint8_t reject_reason) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 1};
    memset(r.external_order_id, 0, MAX_EXTERNAL_ORDER_ID_LENGTH);
    r.exchange_id = cancel_request->exchange_id;
    r.ack_type = REQUEST_REJECT;
    r.reject_reason = reject_reason;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);
    strncpy(r.reject_message, reject_message.c_str(), MAX_REJECT_LENGTH-1);

    // These need to be supplied by the arguments
    r.internal_order_id = cancel_request->internal_order_id;
    r.instrument_id = cancel_request->instrument_id;
    r.strategy_id = cancel_request->strategy_id;
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
// We use this one when we don't have the order details at hand (in websocket for instance)
// =================================================================================
void BaseTradeAdapter::send_exchange_order_ack(char *external_order_id) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.ack_type = EXCHANGE_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);

    auto iter = external_order_id_to_internal_order_id.find(external_order_id);
    if(iter != external_order_id_to_internal_order_id.end()){
        auto internal_order_id = iter->second;
        auto order_details = internal_order_id_to_order[internal_order_id];
        r.internal_order_id         = internal_order_id;
        r.instrument_id             = order_details->instrument_id;
        r.strategy_id               = order_details->strategy_id;
        r.exchange_id               = order_details->exchange_id;
    } else {
        // Can't find the order details - need to send fill with empty details for troubleshooting
        r.internal_order_id         = 0;
        r.instrument_id             = 0;
        r.strategy_id               = 0;
        r.exchange_id               = 0;
    }
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_exchange_order_reject(char *external_order_id, std::string reject_reason_string, uint8_t reject_reason) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.ack_type = EXCHANGE_REJECT;
    r.reject_reason = reject_reason;

    auto iter = external_order_id_to_internal_order_id.find(external_order_id);
    if(iter != external_order_id_to_internal_order_id.end()){
        auto internal_order_id = iter->second;
        auto order_details = internal_order_id_to_order[internal_order_id];
        r.internal_order_id = internal_order_id;
        r.instrument_id = order_details->instrument_id;
        r.strategy_id = order_details->strategy_id;        
        r.exchange_id = order_details->exchange_id;
    } else {
        // Key not found - send message but indicate we have an error
        r.internal_order_id = 0;
        r.instrument_id = 0;
        r.strategy_id = 0;
        r.exchange_id = 0;
    }
    if(reject_reason_string.length() < 100){
        strcpy(r.reject_message, reject_reason_string.c_str());
    } else {
        strncpy(r.reject_message, reject_reason_string.c_str(), 99);
        r.reject_message[99]='\0';
    }
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_exchange_cancel_ack(char *external_order_id) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.ack_type = EXCHANGE_CANCEL_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);

    auto iter = external_order_id_to_internal_order_id.find(external_order_id);
    if(iter != external_order_id_to_internal_order_id.end()){
        auto internal_order_id = iter->second;
        auto order_details = internal_order_id_to_order[internal_order_id];
        r.internal_order_id         = internal_order_id;
        r.instrument_id             = order_details->instrument_id;
        r.strategy_id               = order_details->strategy_id;
        r.exchange_id               = order_details->exchange_id;
    } else {
        // Can't find the order details - need to send fill with empty details for troubleshooting
        r.internal_order_id         = 0;
        r.instrument_id             = 0;
        r.strategy_id               = 0;
        r.exchange_id               = 0;
    }
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}

// =================================================================================
void BaseTradeAdapter::send_exchange_cancel_reject( struct CancelOrder *cancel_request,
                                                    char *external_order_id,
                                                    std::string reject_message,
                                                    uint8_t reject_reason) {
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    strcpy((char *) r.external_order_id, (char *) external_order_id);
    r.ack_type = EXCHANGE_REJECT;
    r.reject_reason = reject_reason;
    memset(r.reject_message, 0, MAX_REJECT_LENGTH);
    strncpy(r.reject_message, reject_message.c_str(), MAX_REJECT_LENGTH-1);

    // These need to be supplied by the arguments
    r.internal_order_id = cancel_request->internal_order_id;
    r.instrument_id = cancel_request->instrument_id;
    r.strategy_id = cancel_request->strategy_id;
    r.exchange_id = cancel_request->exchange_id;
    r.send_timestamp = get_current_ts();
    send_any_io_message((char*)&r, sizeof(r));
}
                                      
                                      
// =================================================================================
void BaseTradeAdapter::send_fill(char *external_order_id, double price, double qty, double leaves_qty, bool is_buy, std::string exchange_trade_id) {
    Fill f;
    f.msg_header = MessageHeaderT{sizeof(Fill), MSG_FILL, 0};
    f.fill_price                = price;
    f.fill_qty                  = qty;
    strcpy((char *) f.external_order_id, (char *) external_order_id);        

    strcpy(f.exchange_trade_id , exchange_trade_id.c_str());
    f.leaves_qty                = leaves_qty;

    auto iter = external_order_id_to_internal_order_id.find(external_order_id);
    if(iter != external_order_id_to_internal_order_id.end()){
        auto internal_order_id = iter->second;
        auto order_details = internal_order_id_to_order[internal_order_id];
        f.internal_order_id         = internal_order_id;
        f.instrument_id             = order_details->instrument_id;
        f.strategy_id               = order_details->strategy_id;
        f.exchange_id               = order_details->exchange_id;
    } else {
        // Can't find the order details - need to send fill with empty details for troubleshooting
        f.internal_order_id         = 0;
        f.instrument_id             = 0;
        f.strategy_id               = 0;
        f.exchange_id               = 0;
    }
    f.send_timestamp = get_current_ts();
    send_any_io_message((char*)&f, sizeof(f));
}
