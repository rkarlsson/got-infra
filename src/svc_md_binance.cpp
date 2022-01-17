#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>
#include <list>
#include <unordered_map>
#include <map>
#include <getopt.h>
#include <chrono>
#include <thread>
#include <curl/curl.h>
#include "wsock.hpp"
#include "refdb.hpp"
#include "logger.hpp"
#include "file_writer.hpp"
#include "aeron_types.hpp"
#include "heartbeat_service.hpp"
#include "binance_md_process.hpp"
#include "MyRingBuffer.hpp"
#include "to_aeron.hpp"

bool all_instruments = false;
char start_letter;
char end_letter;
bool range_given = false;

struct snapshot_info {
    uint8_t ex_id;
    uint32_t instrument_id;
    std::string instrument_name;
    std::string current_date;
    bool to_file;
};
typedef MyRingBuffer<snapshot_info, 1024> SnapshotRingT;

SnapshotRingT snapshot_ring;
std::map<uint32_t, char *> symbol_to_snapshotmessage;

void print_options(){
  std::cout << "Options for svc_md_binance:" << std::endl;
  std::cout << "  -E (--environment) <PROD|UAT>                           = Sets to Prod or UAT config (need one of them)" << std::endl;
  std::cout << "  -r (--range) <StartLetter><StopLetter>                  = Range of start letters to include" << std::endl;
  std::cout << "  -c (--collect)                                          = Write the data to collection output files" << std::endl;
  std::cout << "  -m (--market-data)                                      = Write the data in normalised form on aeron bus" << std::endl;
  std::cout << "  -o (--offset-day)                                       = This is used when starting next days capture early" << std::endl;
  std::cout << "  -a (--all-instruments)                                  = Do all instruments - not just live" << std::endl;
  std::cout << "  -s (--stdout-only)                                      = Only log to stdout instead of influx" << std::endl;
  std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

std::string get_current_date_as_string(int offset) {
    std::string outputstring;
    char buf[100];
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    time_now.tv_sec += (offset * 3600 *24);
    struct tm tstruct = *localtime(&time_now.tv_sec);
    strftime(buf, sizeof(buf), "%Y-%m-%d", &tstruct);
    outputstring = buf;
    return(outputstring);
}

static size_t curl_writemethod(char *buffer, size_t size, size_t nmemb, void *data) {
  std::string *s = (std::string *)data;
  s->append(buffer, size*nmemb);
  return size*nmemb;
}

uint64_t get_current_ts() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return((t.tv_sec*1000000000L)+t.tv_nsec);
}


// -----------------------------------------------------------------------
// This thread processes all snapshot requests given to it in the ringbuffer
// -----------------------------------------------------------------------
void process_snapshot_requests(Logger *snapshot_logger) {
    std::thread snapshot_thread([snapshot_logger]() {
        std::string request_url = "";
        CURL * curl;
        curl = curl_easy_init();
        struct snapshot_info *snap_info;

        while (1){
            if (snapshot_ring.GetPopPtr(&snap_info)) {
                // Process the snapshot request
                if (snap_info->ex_id == 16) {
                    request_url = "https://api.binance.com/api/v3/depth?symbol=" + snap_info->instrument_name + "&limit=1000";
                } else if (snap_info->ex_id == 18) {
                    request_url = "https://fapi.binance.com/fapi/v1/depth?symbol=" + snap_info->instrument_name + "&limit=1000";
                } else {
                    request_url = "https://dapi.binance.com/dapi/v1/depth?symbol=" + snap_info->instrument_name + "&limit=1000";
                }
                snapshot_logger->msg(INFO, "Processing snapshot for: " + snap_info->instrument_name + " - Using URL: " + request_url);
                
                uint64_t timestamp;
                std::string response;
                curl_easy_setopt(curl, CURLOPT_URL, request_url.c_str());
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_writemethod);
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                curl_easy_perform(curl);

                // for capture to_file true
                if(snap_info->to_file){
                    std::string file_name = "/datacollection/binance/" + snap_info->current_date + "_";
                    file_name += snap_info->instrument_name + "_" + std::to_string(snap_info->instrument_id) + "_ss.txt";
                    FileWriter *file_writer = new FileWriter(file_name);

                    timestamp = get_current_ts();
                    file_writer->write_to_file(response, timestamp);
                    delete(file_writer);
                } 
                // Here we handle the writing of the output to a symbol map so the realtime thread can easily check if something is there
                char *snapshot_buffer = (char*) malloc(sizeof(char) * (1024*1024));
                memcpy(snapshot_buffer, response.c_str(), response.length() + 1);
                symbol_to_snapshotmessage[snap_info->instrument_id] = snapshot_buffer;

                snapshot_ring.incrTail();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        }
    });
    snapshot_thread.detach();
}

// -----------------------------------------------------------------------
// This thread builds snapshots and then publishes them onto aeron snapshot message bus
// -----------------------------------------------------------------------
// void snapshot_publisher(Logger *snapshot_logger, WSock *wsocket, char *snapshot_buffer) {
//     std::thread snapshot_publisher_thread([snapshot_logger, wsocket, snapshot_buffer]() {
//         to_aeron *to_aeron_ss;
//         uint64_t sleep_time_in_micros = 0;
//         int num_messages = 0;

//         to_aeron_ss = new to_aeron(AERON_SS);

//         snapshot_logger->msg(INFO, "Starting Snapshot Publisher thread, sleeping for 60 seconds before loop is started");
//         sleep(60);
//         snapshot_logger->msg(INFO, "Snapshot Publisher loop now starting");

//         int bin_snapshot_message_offset = 0;
//         char *snapshot_msg_pointer;
        
//         sleep_time_in_micros = ((uint64_t) 3000000000) / wsocket->num_sockets();
//         // Infinite loop that iterates over all items over and over
//         while (1){
//             num_messages = wsocket->get_next_snapshot(snapshot_buffer);
//             for(int i = 0; i < num_messages; i++){
//                 snapshot_msg_pointer = snapshot_buffer + bin_snapshot_message_offset;
//                 to_aeron_ss->send_data(snapshot_msg_pointer, ((MessageHeader *) snapshot_msg_pointer)->msgLength);
//                 bin_snapshot_message_offset += ((MessageHeader *) snapshot_msg_pointer)->msgLength;
//             }
//             // Recalculate every time, should mean that a snapshot takes ~3 seconds to achieve
//             sleep_time_in_micros = ((uint64_t) 3000000) / wsocket->num_sockets();
//             usleep(sleep_time_in_micros);
//         }
//     });
//     snapshot_publisher_thread.detach();
// }


// -----------------------------------------------------------------------
// This thread listens to snapshot requests and then generates and publishes the snapshot
// -----------------------------------------------------------------------
void snapshot_publisher(Logger *snapshot_logger, WSock *wsocket, char *snapshot_buffer) {
    std::thread snapshot_publisher_thread([snapshot_logger, wsocket, snapshot_buffer]() {
        to_aeron *to_aeron_ss;
        int num_messages = 0;
        uint32_t snap_request_instrument_id = 0;

        aeron::Context                      snap_context;
        std::shared_ptr<Aeron>              snap_aeron = Aeron::connect(snap_context);
        std::int64_t                        snap_channel_id = snap_aeron->addSubscription("aeron:ipc", AERON_SS);
        std::shared_ptr<Subscription>       snap_subscription = snap_aeron->findSubscription(snap_channel_id);

        to_aeron_ss = new to_aeron(AERON_SS);

        while (!snap_subscription) {
            snap_subscription = snap_aeron->findSubscription(snap_channel_id);
        }

        snapshot_logger->msg(INFO, "Snapshot Publisher thread started");

        // Fragment handler lambda, putting it here so I can easily pass extra variables..:)
        auto snap_fragment_lambda = [&snap_request_instrument_id](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
            struct MessageHeader *m = (MessageHeader*)(reinterpret_cast<const char *>(buffer.buffer()) + offset);
            if(m->msgType == DEPTH_SNAPSHOT_REQUEST) {
                struct DepthSnapshotRequest *depth_request = (struct DepthSnapshotRequest*)m;
                snap_request_instrument_id = depth_request->instrument_id;
            }
        };

        
        // Infinite loop that iterates over all items over and over
        while (1){
            // Check if any requests on the bus
            const int fragmentsRead = snap_subscription->poll(snap_fragment_lambda, 10);

            if(snap_request_instrument_id != 0){
                // Build and publish snapshot response
                int bin_snapshot_message_offset = 0;
                char *snapshot_msg_pointer;
                num_messages = wsocket->get_snapshot(snapshot_buffer, snap_request_instrument_id);
                for(int i = 0; i < num_messages; i++){
                    snapshot_msg_pointer = snapshot_buffer + bin_snapshot_message_offset;
                    to_aeron_ss->send_data(snapshot_msg_pointer, ((MessageHeader *) snapshot_msg_pointer)->msgLength);
                    bin_snapshot_message_offset += ((MessageHeader *) snapshot_msg_pointer)->msgLength;
                }
            }
            snap_request_instrument_id = 0;
            usleep(100);
        }
    });
    snapshot_publisher_thread.detach();
}

// -----------------------------------------------------------------------
// Main thread for SVC_MD_BINANCE
// -----------------------------------------------------------------------
int main(int argc, char** argv) {
    std::string_view    data_view;
    WSock               *wsocket;
    RefDB               *refdb;
    std::string         current_date;
    LogWorker           *log_worker;
    Logger              *logger;
    Logger              *subscription_logger;
    to_aeron            *to_aeron_io;
    char snapshot_buffer[1024*1024];


    current_date = get_current_date_as_string(0);
    bool environment_given = false;
    std::string environment_name = "";    

    bool stdout_only = false;

    bool do_collect = false;

    static struct option long_options[] = {
        {"environment"      , optional_argument, NULL, 'E'},
        {"stdout-only"      , optional_argument, NULL, 's'},
        {"offsetday"        , optional_argument, NULL, 'o'},
        {"marketdata"       , optional_argument, NULL, 'm'},
        {"all-instruments"  , optional_argument, NULL, 'a'},
        {"range"            , optional_argument, NULL, 'r'},
        {"collect"          , optional_argument, NULL, 'c'},
        {"help"             , optional_argument, NULL, 'h'}};

    int cmd_option;
    while((cmd_option = getopt_long(argc, argv, "E:shcmoar:", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'E':
                environment_given = true;
                environment_name = optarg;
                break;

            case 'r':
                range_given = true;
                if (strlen(optarg) == 2) {
                    // we are given 2 letters.. good thing
                    start_letter = optarg[0];
                    end_letter = optarg[1];
                } else {
                    // Comaplain and exit
                    std::cout << "Give 2 letters for range.. exiting" << std::endl;
                    exit(1);
                }
                break;

            case 's':
                stdout_only = true;
                break;

            case 'a':
                all_instruments = true;
                break;

            case 'c':
                do_collect = true;
            break;

            case 'm':
                do_collect = false;
            break;

            case 'o':
                current_date = get_current_date_as_string(1);
            break;
            
            case 'h':
                print_options();
                exit(1);

            default:
                break;
        }
    }

    if(! environment_given)
    {
        print_options();
        exit(1);
    }

    if(environment_name == "UAT") {
        log_worker = new LogWorker("svc_md_binance", "All_Binance_Exchanges", "UAT", stdout_only);
        subscription_logger = log_worker->get_new_logger("subscriptions");
        logger = log_worker->get_new_logger("base");    
        refdb = new RefDB("UAT", logger);
    } 
    else {
        log_worker = new LogWorker("svc_md_binance", "All_Binance_Exchanges", "PROD", stdout_only);
        subscription_logger = log_worker->get_new_logger("subscriptions");
        logger = log_worker->get_new_logger("base");
        refdb = new RefDB("PROD", logger);
    } 

    refdb->get_all_instrument_from_db();
    refdb->get_all_exchanges_from_db();

    wsocket = new WSock(logger, subscription_logger, 1800, 50);

    // Start heartbeating
    if(do_collect){
        start_heartbeat(1, CAPTURE_SERVICE);
    } else {
        snapshot_publisher(log_worker->get_new_logger("snapshotpublisher_thread"), wsocket, snapshot_buffer);
        start_heartbeat(1, MARKETDATA_SERVICE);
    }

     // Add subscriptions for all three Binance exchanges
    std::list<std::string> exchange_list {"Binance", "Binance Futures", "BinanceDEX"};
    for (auto exch_name : exchange_list) {
        uint8_t ex_id = refdb->get_exchange_id(exch_name);
        auto instruments = refdb->get_all_symbols_for_exchange(ex_id);

        for(auto const& instrument: instruments) {
            if(instrument->is_live < 2){
                std::string instrument_name = std::string(instrument->instrument_name);
                if(range_given){
                    if ((instrument_name.front() < start_letter) || (instrument_name.front() > end_letter))
                        continue;
                }
                std::string cap_instrument_name = instrument_name;
                std::transform(instrument_name.begin(), instrument_name.end(), instrument_name.begin(), 
                    [](unsigned char c){ return std::tolower(c); });
                logger->msg(INFO, "Adding all subscriptions for Instrument: " + instrument_name);

                std::string file_name = "/datacollection/binance/" + current_date + "_" + std::string(instrument->instrument_name);
                file_name += "_" + std::to_string(instrument->instrument_id) + "_all.txt";

                FileWriter *file_writer;
                if(do_collect) {
                    file_writer = new FileWriter(file_name);
                } else{
                    file_writer = nullptr;
                }

                if(exch_name == "Binance"){
                    wsocket->add_subscription_request("wss://stream.binance.com:9443/ws/" + instrument_name + "@depth@100ms",file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://stream.binance.com:9443/ws/" + instrument_name + "@trade", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://stream.binance.com:9443/ws/" + instrument_name + "@bookTicker", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                }

                else if (exch_name == "Binance Futures"){
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@depth@0ms", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@trade", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@bookTicker", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@markPrice@1s", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@forceOrder", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);                    
                }

                else if (exch_name == "BinanceDEX"){
                    wsocket->add_subscription_request("wss://dstream.binance.com/ws/" + instrument_name + "@depth@0ms", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://dstream.binance.com/ws/" + instrument_name + "@trade", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://dstream.binance.com/ws/" + instrument_name + "@bookTicker", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://dstream.binance.com/ws/" + instrument_name + "@markPrice@1s", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://fstream.binance.com/ws/" + instrument_name + "@forceOrder", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                }            
            }
        }
    }

    std::string_view        message_to_print;
    struct snapshot_info    snap_info;
    DecodeResponse          decode_response;
    DecodeResponse          decode_snapshot_response;
    char                    bin_message_buffer[1024*1024];
    char                    bin_snapshot_buffer[1024*1024];
    auto binance_processor  = new BinanceMDProcessor(bin_message_buffer, bin_snapshot_buffer, &decode_response, &decode_snapshot_response);
    to_aeron_io             = new to_aeron(AERON_IO);

    // This thread processes snapshot requests and writes them to a binary file (collection)
    process_snapshot_requests(log_worker->get_new_logger("snapshot_thread"));


    int bin_message_offset;
    char *msg_pointer;
    

    for(;;) {
        bin_message_offset = 0;
        msg_pointer = bin_message_buffer + bin_message_offset;

        message_to_print = wsocket->get_next_message_from_websocket();

        // Capture process is part of the same pipeline as aeron publisher, enables seq checking..
        if(do_collect){
            wsocket->get_filewriter()->write_to_file(message_to_print, wsocket->get_message_receive_time());
        }

        if(wsocket->in_snapshot_state()){
            if(symbol_to_snapshotmessage.count(wsocket->get_instrument_id())){
                // We have received a snapshotupdate lets process it
                binance_processor->process_message( 
                                std::string_view(symbol_to_snapshotmessage[wsocket->get_instrument_id()]),
                                wsocket->get_message_receive_time(), 
                                wsocket->get_instrument_id(), 
                                wsocket->get_exchange_id(), 
                                wsocket->get_bid_price(), 
                                wsocket->get_ask_price(),
                                true);                

                int bin_snapshot_message_offset = 0;
                char *snapshot_msg_pointer;
                snapshot_msg_pointer = bin_snapshot_buffer + bin_snapshot_message_offset;
                if(! do_collect){
                    // Send instrument clear message
                    InstrumentClearBook clear_msg;
                    clear_msg.msg_header = {sizeof(InstrumentClearBook), INSTRUMENT_CLEAR_BOOK, 1};
                    clear_msg.instrument_id = wsocket->get_instrument_id();
                    clear_msg.exchange_id = wsocket->get_exchange_id();
                    clear_msg.book_type_to_clear = PL_BOOK_TYPE;
                    clear_msg.clear_reason = EXCHANGE_SNAP;
                    clear_msg.sending_timestamp = get_current_ts();
                    wsocket->clear_plbook();
                    to_aeron_io->send_data((char *) &clear_msg, sizeof(InstrumentClearBook));

                    // Loop over the multiple messages that the snapshot will return
                    for(int i = 0; i < decode_snapshot_response.num_messages; i++){
                        snapshot_msg_pointer = bin_snapshot_buffer + bin_snapshot_message_offset;
                        // Send the snapshot to aeron
                        ((PLUpdates *) snapshot_msg_pointer)->sending_timestamp = get_current_ts();
                        wsocket->process_plbook_update((PLUpdates *) snapshot_msg_pointer);
                        to_aeron_io->send_data(snapshot_msg_pointer, ((MessageHeader *) snapshot_msg_pointer)->msgLength);

                        // Update the offset to point to the next one
                        bin_snapshot_message_offset += ((MessageHeader *) snapshot_msg_pointer)->msgLength;
                    }
                }

                // Set the last sequence number of the socket to that of the snapshot
                wsocket->set_last_sequence_number(((PLUpdates *) snapshot_msg_pointer)->end_seq_number);

                // Delete the snapshotupdate from map (first dealloc memory)
                free(symbol_to_snapshotmessage[wsocket->get_instrument_id()]);
                symbol_to_snapshotmessage.erase(wsocket->get_instrument_id());

                // Reset the snapshot state
                wsocket->set_snapshot_state(false);
            }
        }

        // extract data and write to aeron
        binance_processor->process_message( 
                                message_to_print, 
                                wsocket->get_message_receive_time(), 
                                wsocket->get_instrument_id(), 
                                wsocket->get_exchange_id(), 
                                wsocket->get_bid_price(), 
                                wsocket->get_ask_price());


        for(int i = 0; i < decode_response.num_messages; i++){
            msg_pointer = bin_message_buffer + bin_message_offset;
            switch(((MessageHeader *) msg_pointer)->msgType){
                case TOB_UPDATE:
                    if(! do_collect){
                        ((ToBUpdate *) msg_pointer)->sending_timestamp = get_current_ts();
                        to_aeron_io->send_data(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                    }
                    // update what last touch prices were to establish trade sides in decoder
                    wsocket->set_ask_price(((ToBUpdate *) msg_pointer)->ask_price);
                    wsocket->set_bid_price(((ToBUpdate *) msg_pointer)->bid_price);
                    break;

                case PL_UPDATE:
                    if(wsocket->get_last_sequence_number() != decode_response.previous_end_seq_no){
                        if(decode_response.previous_end_seq_no < wsocket->get_last_sequence_number()){
                            std::cout << "Update is older than current seq no. dropping (most likely because of snapshot)" << std::endl;
                        } else{
                            std::cout << "Found a gap: (previous end seq): " << std::to_string(wsocket->get_last_sequence_number());
                            std::cout << " - (current end seq): " << std::to_string(decode_response.previous_end_seq_no);
                            std::cout << " (initiating snapshot)" << std::endl;
                            // lets confirm that this one is a depth feed, if so - lets do a snapshot
                            auto connection = wsocket->get_connection_string();
                            if(connection.find("depth") != connection.npos){
                                // now - enque the snapshot request to the snapshot thread
                                if(! wsocket->in_snapshot_state()){
                                    snap_info.current_date = current_date;
                                    snap_info.ex_id = wsocket->get_exchange_id();
                                    snap_info.instrument_id = wsocket->get_instrument_id();
                                    snap_info.instrument_name = wsocket->get_instrument_name();
                                    if(do_collect){
                                        snap_info.to_file = true;
                                    } else {
                                        snap_info.to_file = false;
                                    }
                                    while(!snapshot_ring.tryEnqueue(std::move(snap_info)));
                                    wsocket->set_snapshot_state(true);
                                }
                            }
                        }
                    }
                    wsocket->set_last_sequence_number(((PLUpdates *) msg_pointer)->end_seq_number);
                    if(! do_collect){
                        // This is  true most of the time - that we want to write the message..
                        ((PLUpdates *) msg_pointer)->sending_timestamp = get_current_ts();
                        wsocket->process_plbook_update((PLUpdates *) msg_pointer);
                        to_aeron_io->send_data(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                    }
                    break;

                case TRADE:
                    if(! do_collect){
                        ((Trade *) msg_pointer)->sending_timestamp = get_current_ts();
                        to_aeron_io->send_data(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                    }
                    break;

                case SIGNAL:
                    if(! do_collect){
                        to_aeron_io->send_data(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                    }
                    break;
            }
            // Increment offset in case there is more than one message in the buffer
            bin_message_offset += ((MessageHeader *) msg_pointer)->msgLength;
        }


    }

    delete(wsocket);
    return(0);
}