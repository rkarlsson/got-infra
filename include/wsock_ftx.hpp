#pragma once

/* the usual suspects */
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <array>
#include <sys/epoll.h>
#include <endian.h>
#include <string_view>
#include <vector>
#include <time.h>
#include <charconv>
#include <cmath> 
#include <map>
#include <chrono>
#include <thread>

/* socket includes */
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

/* wolfSSL */
#include <wolfssl/options.h>
#include <wolfssl/ssl.h>
#include <wolfssl/wolfio.h>
#include <wolfssl/test.h>

// Internal projects
#include "file_writer.hpp"
#include "logger.hpp"
#include "sl.hpp"
#include "MyRingBuffer.hpp"
#include "merged_orderbook.hpp"

#define WOLFSSL_TLS13
#define CERT_FILE "/usr/share/ca-certificates/mozilla/GlobalSign_Root_CA.crt"

#define MAX_EVENTS 1
#define FRAGMENT_BUFFER_SIZE 1024*1024

struct fd_info {
    int fd;
    WOLFSSL *ssl_ptr;
    char connection_string[512];
    char *fragment_buffer;
    uint32_t buffered_size;
    uint32_t buffer_offset;
    uint64_t last_read_time;
    uint64_t last_epoll_time;
    uint64_t last_keepalive;
    bool delete_me;
    bool in_shapshot_state;
    SL pl_lock;
};

struct subscription_info {
    std::string websocket_URI;
    fd_info *socket_info;
};
typedef MyRingBuffer<subscription_info, 16384> SubscriptionRingT;

class WSock {
    private:
        Logger *logger = nullptr;
        Logger *subscription_logger = nullptr;

        SubscriptionRingT   subscription_ring;
        WOLFSSL_CTX         *ctx = NULL;
        std::unordered_map<int, struct fd_info*> socket_to_fd_info;
        SL fd_map_lock;
        std::unordered_map<int, struct fd_info*> snapshot_map;
        std::unordered_map<int, struct fd_info*>::iterator snapshot_map_it;
        
        // This is the delay between subscriptions in order to not oversubscribe
        uint64_t sub_delay_millis = 50;

        int refresh_timeout = 600;

        struct epoll_event EVENTS[MAX_EVENTS];
        int current_socket;
        int epoll_id;
        struct fd_info *current_fd_info;

        std::string_view ws_messages[256];
        int num_messages = 0;
        int message_pointer = 0;
        uint64_t message_receive_time;

        uint64_t last_delete_check_time;

        uint64_t get_current_ts_ns();

        bool add_event_to_socket(struct fd_info *struct_ptr, int event_to_remove);
        bool remove_event_from_socket(struct fd_info *struct_ptr, int event_to_remove);
        bool modify_event_on_socket(struct fd_info *struct_ptr, int event_to_remove);
        int get_new_socket(struct sockaddr* sock_addr, fd_info *socket_info);

        int read();
        int ws_read();

        int write_ssl(char *stuff_to_write, int length_of_data_to_write, fd_info *socket_info);

        bool subscribe(std::string connection_string, fd_info *socket_inf, std::string websocket_hostname_string);
        bool send_pong(char *msg_ptr, int msg_len);
        void send_keepalive_pong(fd_info *socket_info);
        void process_subscription_requests();
        // bool connect_to_websocket(std::string websocket_URI, uint32_t instrument_id);
        struct fd_info* connect_to_websocket(std::string websocket_URI);

    public:
        WSock(Logger *_logger, Logger *_subscription_logger, int refresh_time, int subscription_delay);
        ~WSock();

        void add_subscription_request(std::string websocket_URI, fd_info *socket_info = NULL);
        std::string_view get_next_message_from_websocket();
        uint64_t get_message_receive_time();
        bool in_snapshot_state();
        void set_snapshot_state(bool new_snapshot_state);
        std::string get_connection_string();
        FileWriter *get_filewriter();

        // All related to PL publishing and snapshotting of the same
        void aquire_plbook_lock();
        void release_plbook_lock();
        void process_plbook_update(PLUpdates *pl_update);
        void clear_plbook();
        int num_sockets();
        int get_snapshot(char *snap_buffer, uint32_t instrument_id);
};
