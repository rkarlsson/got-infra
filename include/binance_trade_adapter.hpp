#pragma once

#include <pthread.h>
#include <csignal>
#include "base_trade_adapter.hpp"
#include "wsock.hpp"
#include <unordered_map>


class BinanceTradeAdapter : public BaseTradeAdapter {
  private:
 

    // API KEY and SECRET to the exchange accounts (same for all three excahgnes)
    std::string API_KEY;
    std::string SECRET_KEY;

    // This map is for all websocket listenkeys
    struct user_websocket_info {
      std::string listenKey;
      std::string rest_endpoint;
      std::string rest_endpoint_margin;
      std::string rest_endpoint_margin_v2;
      std::string ws_endpoint;
      std::string exchange_name;
    };
    std::unordered_map<uint8_t, user_websocket_info*> listenkey_map;

    // Generic for risk management
    CURL *curl;

    CURL *new_order_curl;
    struct curl_slist *new_order_chunk = NULL;
    std::string new_order_response;

    CURL *cancel_curl;
    struct curl_slist *cancel_chunk = NULL;
    std::string cancel_response;

    WSock *user_websockets;

    simdjson::dom::parser parser;

    struct timespec t;
    uint64_t receive_ts;
    uint64_t send_ts;

    uint32_t pong_counter = 0;

    pthread_t user_websocket_thread_handle;

    std::string get_listen_key(std::string rest_endpoint, uint8_t exchange_id);
    void add_user_websocket(Logger *logger, uint8_t exchange_id, std::string ex_name);
    void load_all_open_orders(Logger *logger, uint8_t exchange_id);
    void user_websocket_loop_thread(Logger *logger);
    void listenkey_refresh_thread(Logger *logger);
    fragment_handler_t aeron_msg_handler();

  public:
    BinanceTradeAdapter(  std::string location, 
                          uint8_t order_environment);
};