#pragma once

#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <unistd.h>
#include <charconv>
#include <sstream>
#include <array>
#include <iomanip>
#include <string.h>
#include <fstream>
#include <algorithm>
#include "sl.hpp"
#include "logger.hpp"
#include <random>
#include "wolfssl/wolfcrypt/hmac.h"
// #include <openssl/hmac.h>

#include "Aeron.h"
#include "from_aeron.hpp"
#include "to_aeron.hpp"
#include "config_db.hpp"
#include "double_to_ascii.hpp"

// These will be used by most, so including here
#include <curl/curl.h>
#include "simdjson.h"


// This represents the state of the exchange connection
enum trade_adapter_e {
  _NOT_CONNECTED,
  LISTEN,
  GET_OPEN_ORDERS,
  CONNECTED
};

class BaseTradeAdapter {
  private:
    std::atomic<trade_adapter_e> _state;


    // Private Reference data structures
    std::unordered_map<uint8_t, StrategyInfoResponse *> strategy_info_map;

    uint8_t         _order_environment;

    // This one holds all the configuration for the application
    ConfigDB *config_items;

    // These are used for web initiated orders or auto close orders (liquidation)
    uint64_t wash_book_order_id = 0;
    std::unordered_map<std::string, uint64_t> washbook_external_to_internal_order_id;

  protected:
    std::atomic<bool> got_hb = false;

    // Aeron connectors
    to_aeron *to_aeron_io;
    from_aeron *from_aeron_io;
    std::function<fragment_handler_t()> fragment_handler;
    SL lock;
    void send_any_io_message(char *message_ptr, int length);

    // Moving these over from aeron_oe to track internally instead
    std::unordered_map<uint64_t, struct SendOrder *> internal_order_id_to_order;
    std::unordered_map<std::string, struct SendOrder *> external_order_id_to_order;
    std::unordered_map<std::string, struct SendOrder *> exchange_order_id_to_order;

    std::unordered_map<uint64_t, std::string> internal_order_id_to_external_order_id;
    std::unordered_map<std::string, uint64_t> external_order_id_to_internal_order_id;

    /////////////////////////////
    // LogWorker and Logger - worker runs a thread and writes to a unified output
    // reads from a ringbuffer in order to offload the trading threads
    /////////////////////////////
    LogWorker *log_worker;
    Logger *logger;

    /////////////////////////////
    // Public Reference data structures
    /////////////////////////////
    std::unordered_map<uint32_t, InstrumentInfoResponse *> instrument_info_map;
    std::unordered_map<int, std::string> instr_id_to_instr_name;
    std::unordered_map<std::string, int> instr_name_to_instr_id;
    std::unordered_map<std::string, uint32_t> base_name_to_asset_id;
    std::unordered_map<uint32_t, std::string> asset_id_to_base_name;

    /////////////////////////////
    // These are required for unique orders to market
    /////////////////////////////
    std::string oe_unique_order_id;
    const int unique_part_for_order_id = 8;
    int32_t external_order_id = 0;

  public:
    /////////////////////////////
    // Constructors
    /////////////////////////////
    BaseTradeAdapter(uint8_t order_environment, std::string exchange_name);

    // Aeron message senders - moved from aeron_oe so we can keep internal structures within the process
    void send_internal_order_ack( char *external_order_id,
                            struct SendOrder *send_order);

    void send_internal_order_reject(  struct SendOrder *send_order,
                                std::string reject_message,
                                uint8_t reject_reason);

    void send_internal_cancel_ack(struct CancelOrder *cancel_request, char *external_order_id);

    void send_internal_cancel_reject(  struct CancelOrder *cancel_request,
                                std::string reject_message,
                                uint8_t reject_reason);

    void send_exchange_order_ack(char *external_order_id);
    void send_exchange_order_reject(  char *external_order_id, 
                                std::string reject_reason_string, 
                                uint8_t reject_reason);
        
    void send_exchange_cancel_ack(char *external_order_id);

    void send_exchange_cancel_reject( struct CancelOrder *cancel_request,
                                      char *external_order_id,
                                      std::string reject_message,
                                      uint8_t reject_reason);
        
    void send_fill( char *external_order_id, 
                    double price, 
                    double qty,
                    double leaves_qty,
                    bool is_buy, 
                    std::string exchange_trade_id);

    /////////////////////////////
    // Setters for the private base class variables
    /////////////////////////////
    // State of exchange_connection
    void set_exchange_state(trade_adapter_e new_state);

    /////////////////////////////
    // Getters from the private baseclass variables
    /////////////////////////////
    trade_adapter_e get_exchange_state();
    uint8_t get_order_env();

    /////////////////////////////
    // Callbacks for messages from aeron_risk (allows for instrument/strategy info updates and account_info requests)
    /////////////////////////////
    void update_instrument_info(struct InstrumentInfoResponse *instrument_info); 
    void fetch_instruments();

    void update_strategy_info(struct StrategyInfoResponse *strategy_info);
    void fetch_strategies();

    bool account_info_request(struct AccountInfoRequest *account_req);
    void send_account_update(uint32_t asset_id, uint32_t instrument_id, double value, uint32_t account_info_type, uint8_t update_reason, uint8_t exchange_id);

    void send_margin_info_response(MarginInfoResponse *margin_response);
    void send_margin_borrow_response(MarginBorrowResponse *margin_response);
    void send_margin_transfer_response(MarginTransferResponse *margin_response);
    void send_risk_request_reject(RiskRequestReject *reject);
    void send_margin_alert(AlertMarginCall *alert);
    bool margin_info_request(struct MarginInfoRequest *margin_req);
    bool margin_transfer_request(struct MarginTransferRequest *margin_req);
    bool margin_borrow_request(struct MarginBorrowRequest *margin_req);

    /////////////////////////////
    // Riskcheck methods
    /////////////////////////////
    bool order_pass_riskcheck(struct SendOrder *order_to_send, std::string &reject_message, uint8_t &reject_reason);

    /////////////////////////////
    // Helper methods
    /////////////////////////////
    // Creates signature
    std::string hmacHex(std::string key, std::string msg);
    int hmacHex(std::string key, std::string msg, char *output_string);

    // Gets current ts in nanoseconds
    static uint64_t get_current_ts(); // nanoseconds
    static uint64_t get_current_ts_millis(); // millis

    // Can be used to generate random order seed
    std::string generate_hash_for_oe(int unique_part_for_order_id);
    // Curl needs a write function. We will make use of CURL for a while so may as well add as helper
    static size_t curl_write_func(char *buffer, size_t size, size_t nmemb, void *data);
    // Convert string_view to std::string
    std::string as_string(std::string_view v);
    // String conversion to LSE or double
    uint64_t ascii_to_lserep( const char * str );
    double ascii_to_double( const char * str );
    void uint_to_ascii(unsigned int val, char* c);
    int convert_to_hex(char *buffer, unsigned char *array_to_convert, int length_to_convert);
   
    /////////////////////////////
    // Configuration tools
    /////////////////////////////
    // Get value for key and instance
    std::string get_config_value(std::string key, std::string instance_name);
    // This is used to get all the configuration for this instances at startup
    bool load_config(std::string app_name);

    /////////////////////////////
    // Washbook order_id management
    /////////////////////////////
    // This gets next order id - also adds the external order_id to the lookup map
    uint64_t get_next_washbook_internal_order_id(std::string external_order_id, uint8_t exchange_id);
    // looks up the internal order id from the external
    uint64_t get_internalid_from_external_map(std::string external_id);
};