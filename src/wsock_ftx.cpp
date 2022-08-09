#include "wsock_ftx.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
WSock::WSock(Logger *_logger, Logger *_subscription_logger, int refresh_time, int subscription_delay_milli) {
    int ret;

    logger = _logger;
    logger->msg(INFO, "Setting refresh_timeout to: " + std::to_string(refresh_time));
    subscription_logger = _subscription_logger;
    refresh_timeout = refresh_time;

    /* Initialize wolfSSL */
    if (wolfSSL_Init() != WOLFSSL_SUCCESS) {
        logger->msg(ERROR, "Failed to initialize the library");
        exit(1);
    }

    /* Create and initialize WOLFSSL_CTX */
    if ((ctx = wolfSSL_CTX_new(wolfTLSv1_2_client_method())) == NULL) {
        logger->msg(ERROR, "Failed to create WOLFSSL_CTX");
        exit(1);
    }

    wolfSSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, 0);

    /* Load client certificates into WOLFSSL_CTX */
    if ((ret = wolfSSL_CTX_load_verify_locations(ctx, CERT_FILE, NULL)) != WOLFSSL_SUCCESS) {
        logger->msg(ERROR, "failed to load Certfile, please check the file.");
        exit(1);
    }

    // Setup the epoll ID
    epoll_id = epoll_create(256);
    if(epoll_id < 0){
        logger->msg(ERROR, "Detected issues with epoll_create");
        exit(1);
    }
    memset(&EVENTS, 0, MAX_EVENTS * sizeof(struct epoll_event));

    sub_delay_millis = subscription_delay_milli;
    last_delete_check_time = get_current_ts_ns();
    process_subscription_requests();
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
WSock::~WSock() {
    if(socket_to_fd_info.size() > 0) {
        for (auto const& [key, val] : socket_to_fd_info){

            close(key);          /* Close the connection to the server       */
            wolfSSL_free(((struct fd_info *) val)->ssl_ptr);   /* Free the wolfSSL object                */
        }
    }

    if (ctx)
        wolfSSL_CTX_free(ctx);   /* Free the wolfSSL context object          */
    
    wolfSSL_Cleanup();          /* Cleanup the wolfSSL environment          */
}

// -----------------------------------------------------------------------
// Creates a new socket and returns it - optimised with no nagle etc
// -----------------------------------------------------------------------
int WSock::get_new_socket(struct sockaddr* sock_addr, fd_info *socket_info) {
    int return_socket;
    // struct sockaddr_in  servAddr;
    int ret;
    WOLFSSL *ssl;

    int on = 1;
    socklen_t len = sizeof(on);

    /* Create a socket that uses an internet IPv4 address,
     * Sets the socket to be stream based (TCP),
     * 0 means choose the default protocol. */
    if ((return_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        subscription_logger->msg(ERROR, "Failed to create the socket");

    socket_info->fd = return_socket;

    if (setsockopt(return_socket, IPPROTO_TCP, TCP_NODELAY, &on, len) < 0)
        subscription_logger->msg(ERROR, "Failed to set setsockopt TCP_NODELAY");

    /* Connect to the server */
    if (connect(return_socket, (struct sockaddr*) sock_addr, sizeof(struct sockaddr)) == -1) {
        subscription_logger->msg(ERROR, "Failed to connect to the socket");
    }

    /* Create a WOLFSSL object */
    if ((ssl = wolfSSL_new(ctx)) == NULL) {
        subscription_logger->msg(ERROR, "Failed to create WOLFSSL object");
    }

    socket_info->ssl_ptr = ssl;

    /* Attach wolfSSL to the socket */
    if (wolfSSL_set_fd(ssl, return_socket) != WOLFSSL_SUCCESS) {
        subscription_logger->msg(ERROR, "Failed to set the WOLFSSL socket filedescriptor");
    }

    /* Connect to wolfSSL on the server side */
    if ((ret = wolfSSL_connect(ssl)) != WOLFSSL_SUCCESS) {
        subscription_logger->msg(ERROR, "Failed to setup the TLS handshake with remote host");
        char buff[256];
        int err = wolfSSL_get_error(ssl, ret);
        wolfSSL_ERR_error_string(err, buff);
        std::stringstream errortext;
        errortext << "TLS connection: " << buff << " (" << std::to_string(err) << ")" << std::endl;
        subscription_logger->msg(ERROR, errortext.str());
    }

    socket_info->fragment_buffer = (char *) malloc(FRAGMENT_BUFFER_SIZE);
    socket_info->buffered_size = 0;
    socket_info->delete_me = false;
    socket_info->in_shapshot_state = false;

    return(return_socket);
}

// -----------------------------------------------------------------------
// Returns the current time in nanoseconds
// -----------------------------------------------------------------------
uint64_t WSock::get_current_ts_ns() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return((t.tv_sec*1000000000L)+t.tv_nsec);
}

// -----------------------------------------------------------------------
// Adds an event to epoll monitoring
// -----------------------------------------------------------------------
bool WSock::add_event_to_socket(struct fd_info *struct_ptr, int event_to_add) {
    struct epoll_event event_struct;
    memset(&event_struct, 0, sizeof(event_struct));
    event_struct.data.ptr = struct_ptr;
    event_struct.events = event_to_add;

    auto epoll_ret = epoll_ctl(epoll_id, EPOLL_CTL_ADD, struct_ptr->fd, &event_struct);
    if(epoll_ret < 0) {
        subscription_logger->msg(ERROR, "Epoll_ctl for add failed: " + std::to_string(epoll_ret));
        return(false);
    }
    return(true);
}

// -----------------------------------------------------------------------
// Removes an event from epoll monitoring
// -----------------------------------------------------------------------
bool WSock::remove_event_from_socket(struct fd_info *struct_ptr, int event_to_remove) {
    struct epoll_event event_struct;
    memset(&event_struct, 0, sizeof(event_struct));
    event_struct.data.ptr = struct_ptr;
    event_struct.events = event_to_remove;

    auto epoll_ret = epoll_ctl(epoll_id, EPOLL_CTL_DEL, struct_ptr->fd, &event_struct);
    if(epoll_ret < 0) {
        subscription_logger->msg(ERROR, "Epoll_ctl for remove failed: " + std::to_string(epoll_ret));
        subscription_logger->msg(ERROR, "Errno: " + std::to_string(errno));
        return(false);
    }
    return(true);
}

// -----------------------------------------------------------------------
// Modifies an event being epoll monitored
// -----------------------------------------------------------------------
bool WSock::modify_event_on_socket(struct fd_info *struct_ptr, int new_event) {
    struct epoll_event event_struct;
    memset(&event_struct, 0, sizeof(event_struct));
    event_struct.data.ptr = struct_ptr;
    event_struct.events = new_event;

    auto epoll_ret = epoll_ctl(epoll_id, EPOLL_CTL_MOD, struct_ptr->fd, &event_struct);
    if(epoll_ret < 0) {
        subscription_logger->msg(ERROR, "Epoll_ctl for modify failed: " + std::to_string(epoll_ret));
        return(false);
    }
    return(true);
}

// -----------------------------------------------------------------------
// Sends upgrade and subscribes
// -----------------------------------------------------------------------
bool WSock::subscribe(std::string connection_string, fd_info *socket_info, std::string websocket_hostname_string) {
    printf("Hsfksdgksdgk\n");
    std::string send_string = "GET " + connection_string + " HTTP/1.1";
    send_string += "Host: " + websocket_hostname_string + "\n";
    send_string += "Connection: Upgrade\n";
    send_string += "Pragma: no-cache\n";
    send_string += "Cache-Control: no-cache\n";
    send_string += "Upgrade: websocket\n";
    send_string += "Sec-WebSocket-Version: 13\n";
    send_string += "Sec-WebSocket-Key: q4xkcO32u266gldTuKaSOw==\n\n";

    printf("cs = %s %s\n", connection_string.c_str(), send_string.c_str());

    // auto write_length = write_ssl((char *) send_string.c_str(), send_string.length(), socket_info);
    write_ssl((char *) send_string.c_str(), send_string.length(), socket_info);
    current_fd_info = socket_info;
    char readbuffer[1024];

    int read_length = 0;
    /* Read the server data into our buff array */
    memset(readbuffer, 0, 1024);
    read_length = wolfSSL_read(socket_info->ssl_ptr, readbuffer, 1023);
    if(read_length < 0) {
        logger->msg(ERROR, "Failed to read");
    }
    // std::cout << "READ: " << std::string(readbuffer) << std::endl;
 
    return(true);
}


// -----------------------------------------------------------------------
// Takes a websocket URI as input and connects to it
// -----------------------------------------------------------------------
struct fd_info* WSock::connect_to_websocket(std::string websocket_URI) {
    std::string socket_type;
    std::string hostname_string;
    std::string websocket_hostname_string;
    std::string relative_URI = "";
    std::string port_details;
    int connection_port;
    struct fd_info *connection_info;

    connection_info = new fd_info();
    connection_info->last_read_time = get_current_ts_ns();
    connection_info->last_epoll_time = get_current_ts_ns();
    connection_info->last_keepalive = get_current_ts_ns();
    memcpy(connection_info->connection_string, websocket_URI.c_str(), websocket_URI.length());

    auto found = websocket_URI.find(":");
    auto hostname_start = found + 3; // (add the 2 backslashes)
    if (found!=std::string::npos){
        socket_type = websocket_URI.substr(0, found);
        if (socket_type == "wss"){
            // This is a TLS encrypted websocket connection
            connection_port = 443;
            port_details = "443";
        } else if (socket_type == "ws") {
            // This is not a ssl connection
            connection_port = 80;
            port_details = "80";
        } else {
            // unknown - return false
            return(nullptr);
        }
    }

    found = websocket_URI.find("/",hostname_start);
    if (found!=std::string::npos){
        hostname_string = websocket_URI.substr(hostname_start, found - hostname_start);
        relative_URI = websocket_URI.substr(found, websocket_URI.length() - found);
        websocket_hostname_string = hostname_string;
        // Now check if this ends with port details or not
        found = hostname_string.find(":");
        if (found!=std::string::npos){
            // there are port details - lets extract it
            // lets also trim the hostname from port details
            port_details = hostname_string.substr(found+1);
            hostname_string = hostname_string.substr(0,found);
            std::from_chars(port_details.data(), port_details.data()+port_details.size(), connection_port);
        }
    } else {
        return(nullptr);
    }

    // now we have extracted hostname + port, we need to conver the hostname into a ip address
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;    /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* Datagram socket */
    hints.ai_flags = 0;
    hints.ai_protocol = 0;          /* Any protocol */
    auto s = getaddrinfo(hostname_string.c_str(), port_details.c_str(), &hints, &result);
    if (s != 0) {
        std::stringstream errortext;
        errortext << "getaddrinfo: " << gai_strerror(s);
        subscription_logger->msg(ERROR, errortext.str());
        exit(EXIT_FAILURE);
    }

    int new_socket;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        new_socket = get_new_socket(rp->ai_addr, connection_info);
        if (new_socket == -1)
            continue;
        else
            break;
    }
    subscribe(relative_URI, connection_info, websocket_hostname_string);

    add_event_to_socket(connection_info, EPOLLIN | EPOLLERR | EPOLLPRI | EPOLLRDHUP | EPOLLHUP);

    fd_map_lock.acquire_lock();
    socket_to_fd_info[new_socket] = connection_info;
    fd_map_lock.release_lock();

    return(connection_info);
}



// -----------------------------------------------------------------------
// This processes all subscription requests
// -----------------------------------------------------------------------
void WSock::process_subscription_requests() {
    printf("do this\n");
    std::thread subscription_thread([this]() {
        std::vector<struct fd_info*> fds_to_remove;

        while (1){
            auto current_ts = get_current_ts_ns();
            for (auto& it: socket_to_fd_info) {
                long long int diff = current_ts - it.second->last_read_time;
                if(std::abs(diff) > ((uint64_t) refresh_timeout * 1000000000L))
                {
                    // We should really reconnect and ignore this one
                    std::string event_msg = "Reached max limit of no data - " + std::to_string(refresh_timeout) + " seconds - reconnecting to: " + std::string(it.second->connection_string);
                    subscription_logger->msg(INFO, event_msg);
                    fds_to_remove.push_back(it.second);
                    std::string connection_string(it.second->connection_string);
                    add_subscription_request(   connection_string, 
                                                it.second);
                } 

                if(it.second->delete_me) {
                    fds_to_remove.push_back(it.second);
                }
            }

            for (auto& it: fds_to_remove) {
                if(socket_to_fd_info.count(it->fd)){
                    remove_event_from_socket(it, EPOLLIN | EPOLLERR | EPOLLPRI | EPOLLRDHUP | EPOLLHUP);
                    wolfSSL_shutdown(it->ssl_ptr);
                    wolfSSL_free(it->ssl_ptr);
                    free(it->fragment_buffer);
                    close(it->fd);
                    fd_map_lock.acquire_lock();
                    socket_to_fd_info.erase(it->fd);
                    fd_map_lock.release_lock();
                }
            }
            fds_to_remove.clear();

            // For all remaining sockets - check if 5 minutes has gone and if so - send heartbeat
            for (auto& it: socket_to_fd_info) {
                long long int diff = current_ts - it.second->last_keepalive;
                if(std::abs(diff) > 300000000000) {
                    send_keepalive_pong(it.second);
                    it.second->last_keepalive = get_current_ts_ns();
                }
            }

            struct subscription_info *sub_info;
            if (subscription_ring.GetPopPtr(&sub_info)) {
                // Process the subscription request
                printf("subscribe\n");

                subscription_logger->msg(INFO, "Subscribing to: " + sub_info->websocket_URI);
                auto fd_info = connect_to_websocket(sub_info->websocket_URI);
                subscription_ring.incrTail();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sub_delay_millis));
        }
    });
    subscription_thread.detach();
}


// -----------------------------------------------------------------------
// Writes to the encrypted connection
// -----------------------------------------------------------------------
int WSock::write_ssl(char *stuff_to_write, int length_of_data_to_write, fd_info *socket_info) {
    printf("socket info = %d\n", socket_info);
    WOLFSSL *ssl = socket_info->ssl_ptr;
    int length_written = 0;
    length_written = wolfSSL_write(ssl, stuff_to_write, length_of_data_to_write);
    if (length_written != length_of_data_to_write) {
        int tmp = wolfSSL_get_error(ssl, length_written);
        logger->msg(ERROR, "Failed to write entire message to ssl channel on: " + std::string(socket_info->connection_string));
    }
    return(length_written);
}

// -----------------------------------------------------------------------
// Reads from the ecrypted connection
// -----------------------------------------------------------------------
int WSock::read() {
    int read_length = 0;

    while(read_length <= 0){
        int num_events = 0;
        num_events = epoll_wait(epoll_id, EVENTS, MAX_EVENTS, -1);
        // do
        // {
        //     num_events = epoll_wait(epoll_id, EVENTS, MAX_EVENTS, 0);
        // } while (num_events == 0);

        if (EVENTS[0].events & EPOLLIN) {
            current_fd_info = (struct fd_info *) EVENTS[0].data.ptr;
            current_socket = current_fd_info->fd;
            WOLFSSL *ssl = current_fd_info->ssl_ptr;

            read_length = 0;

            if(current_fd_info->delete_me){
                // Lets not try to read from sockets that has "delete_me" flag set
                continue;
            }

            read_length = wolfSSL_recv( ssl, 
                                        current_fd_info->fragment_buffer + current_fd_info->buffered_size, 
                                        FRAGMENT_BUFFER_SIZE - current_fd_info->buffered_size, 
                                        MSG_DONTWAIT);
            if (read_length < 0) {
                char buff[256];
                int err = wolfSSL_get_error(ssl, read_length);
                if(err == SSL_ERROR_WANT_READ) {
                    // This is OK, just try again
                    // logger->msg(INFO, "WANT READ FOUND - No issues but if often we should check why for: " + std::string(current_fd_info->connection_string));
                    continue;
                }
                else if(err == SSL_ERROR_ZERO_RETURN) { // This means TLS disconnect - need to reconnect
                    logger->msg(ERROR, "ZERO RETURN ON TLS - DROPPING THIS CONNECTION AND RECONNECTING");
                    if(! current_fd_info->delete_me){
                        current_fd_info->delete_me = true;
                        std::string connection_string(current_fd_info->connection_string);
                        add_subscription_request(connection_string, current_fd_info);
                    }
                } 
                else {
                    wolfSSL_ERR_error_string(err, buff);
                    std::stringstream errortext;
                    errortext << "failed to read: " << buff << " (" << std::to_string(err) << ")";
                    logger->msg(ERROR, errortext.str());
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);                    
                }
            }
        } 
        else {
            if(EVENTS[0].events & EPOLLRDHUP) {
                logger->msg(WARN, "Unexpected close (EPOLLRDHUP)");
                if(! current_fd_info->delete_me){
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);
                }
            }
            else if(EVENTS[0].events & EPOLLERR) {
                logger->msg(WARN, "Unexpected close (EPOLLERR)");
                if(! current_fd_info->delete_me){
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);
                }
            }
            else if(EVENTS[0].events & EPOLLPRI) {
                logger->msg(WARN, "Unexpected close (EPOLLPRI)");
                if(! current_fd_info->delete_me){
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);
                }
            }
            else if(EVENTS[0].events & EPOLLHUP) {
                logger->msg(WARN, "Unexpected close (EPOLLHUP)");
                if(! current_fd_info->delete_me){
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);
                }
            }
            else {
                logger->msg(WARN, "Unexpected event from epoll_wait: " + std::to_string(EVENTS[0].events));
            }

        }
    }
    return(read_length);
}

// -----------------------------------------------------------------------
// Responds back with a pong to current socket
// -----------------------------------------------------------------------
bool WSock::send_pong(char *msg_ptr, int msg_len) {
    msg_ptr[0] = 128 + 10; // fin bit set and 10 = pong op_code
    msg_ptr[1] |= 1UL << 7;
    write_ssl(msg_ptr, msg_len, current_fd_info);
    return(true);
}

bool WSock::send_data(char *msg_ptr, int msg_len) {
    write_ssl(msg_ptr, msg_len, current_fd_info);
    return(true);
}


// -----------------------------------------------------------------------
// Sends unsolicited pongs to a socket in order to keep them alive
// -----------------------------------------------------------------------
void WSock::send_keepalive_pong(fd_info *socket_info) {
    char pong_response[2];
    pong_response[0] = 128 + 10; // fin bit set and 10 = pong op_code
    pong_response[1] = 128;
    write_ssl(pong_response, 2, socket_info);
}

// -----------------------------------------------------------------------
// Reads from the ecrypted connection
// -----------------------------------------------------------------------
int WSock::ws_read() {
    int read_length;
    uint64_t payload_length;

    int offset = 2; // ws header is minimum of 2 bytes

    num_messages = 0; // reset message counter
    // Extract all messages from the read into the message array
    // Then the consumer can just get message by message from the array
    while(num_messages == 0){
        bool fragmented_packet = false;
        char *message_char_ptr;
        read_length = read();
        message_receive_time = get_current_ts_ns();

        if(current_fd_info->buffered_size != 0){
            // There is already data in the fragment buffer, lets add this to the end and process the buffer instead
            logger->msg(INFO, "Adding to buffered fragment on: " + std::string(current_fd_info->connection_string));
            auto init_read_length = read_length;
            read_length += current_fd_info->buffered_size;
            current_fd_info->buffered_size += init_read_length;
        }

        // Initialise the message pointer
        message_char_ptr = current_fd_info->fragment_buffer + current_fd_info->buffer_offset;

        // Process the data read
        do {
            int op_code = message_char_ptr[0] & 15;
            bool fin_bit = message_char_ptr[0] & 128;
            bool masking = message_char_ptr[1] & 128;
            offset = 2;

            payload_length = message_char_ptr[1] & 127;
            if (payload_length == 126) { // 2 bytes extended payload
                payload_length = be16toh(*((uint16_t *) (message_char_ptr + 2)));
                offset += 2;
            }
            else if (payload_length == 127) { // 8 bytes extended payload
                payload_length = be64toh(*((uint64_t *) (message_char_ptr + 2)));
                offset += 8;
            }
            if (masking){
                logger->msg(INFO, "Found data that was masked, we do not support that right now");
                offset += 4;
                // TODO we should get the masking key here as well
            }

            // Handle fragmentation - if payload indicates that it doesn't fit in what is left of the buffer
            if((payload_length + offset) > (read_length - current_fd_info->buffer_offset)){
                // logger->msg(INFO, "Payload larger than data left on buffer on following websocket: " + std::string(current_fd_info->connection_string));
                fragmented_packet = true;
                if (current_fd_info->buffered_size == 0) {
                    // We need to buffer this (first time)
                    current_fd_info->buffered_size += read_length;
                    break;
                }

                // we will just break here - we have already buffered the message in the beginning
                break;
            }

            if((op_code == 1) && (fin_bit)){
                // We only send this to the parser if it is text and it is the final segment in the message
                ws_messages[num_messages++] = std::string_view(message_char_ptr + offset, payload_length);
                current_fd_info->last_read_time = message_receive_time;
            }
            else if (op_code == 2){
                logger->msg(INFO, "Received binary data on: " + std::string(current_fd_info->connection_string));
            }
            else if (op_code == 9){
                // logger->msg(INFO, "Sending Pong on: " + std::string(current_fd_info->connection_string));
                send_pong(message_char_ptr, offset + payload_length);
            }
            else if (op_code == 8){
                logger->msg(INFO, "Received connection closed on websocket: " + std::string(current_fd_info->connection_string));
                // clean_and_resubscribe(current_fd_info);
                if(! current_fd_info->delete_me){
                    current_fd_info->delete_me = true;
                    std::string connection_string(current_fd_info->connection_string);
                    add_subscription_request(connection_string, current_fd_info);
                }
            }
            else if (op_code == 0){
                logger->msg(INFO, "Found a continuation frame, I hope things were buffered as part of it");
            }            
            else {
                logger->msg(INFO, "Unknown op_code on: " + std::string(current_fd_info->connection_string));
            }

            current_fd_info->buffer_offset += payload_length + offset;
            message_char_ptr = current_fd_info->fragment_buffer + current_fd_info->buffer_offset;

        } while( (read_length - current_fd_info->buffer_offset) > 0);

        if(!fragmented_packet) {
            // reset this variable every time
            current_fd_info->buffered_size = 0;
            // reset message pointer
            current_fd_info->buffer_offset = 0;
        }
    }

    return(num_messages);
}


// -----------------------------------------------------------------------
// Takes a websocket URI as input and adds to subscriptionrequest ring
// -----------------------------------------------------------------------
void WSock::add_subscription_request(std::string websocket_URI, fd_info *socket_info) {
    printf("added\n");
    // I use this one so it is shared between all sockets that belong to a single symbol ID
    // Useful when looking up ToB value for a Trade so I can distinguish the side value of the trade
    // Trades are published on a separate socket and would otherwise have to be looped up every time in a map, very slow and resource intense

    struct subscription_info new_request;

    new_request.websocket_URI = websocket_URI;
    new_request.socket_info = socket_info;
    while(!subscription_ring.tryEnqueue(std::move(new_request)));
}

std::string_view WSock::get_next_message_from_websocket() {
    if (message_pointer == num_messages){
        ws_read();
        message_pointer = 0; // reset pointer to array
    }
    return(ws_messages[message_pointer++]);
}


