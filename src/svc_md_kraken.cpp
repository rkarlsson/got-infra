#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>
#include <list>
#include <unordered_map>
#include <getopt.h>
#include <curl/curl.h>
#include "wsock2.hpp"
#include "refdb.hpp"
#include "logger.hpp"
#include "file_writer.hpp"
#include "aeron_types.hpp"
#include "heartbeat_service.hpp"
#include "kraken_md_process.hpp"

bool all_instruments = false;
char start_letter;
char end_letter;
bool range_given = false;

void print_options(){
  std::cout << "Options for svc_md_kraken:" << std::endl;
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
    strftime(buf, sizeof(buf), "%Y%m%d", &tstruct);
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

void snapshot_thread(RefDB *refdb, std::string current_date) {

    struct snapshot_info {
        FileWriter *file_writer;
        std::string request_url;
    };

    std::list<struct snapshot_info*> snap_list;

    // Create all snapshot filewriters and urls for the instruments and push them into a list I can iterate over later
    std::list<std::string> exchange_list {"Kraken", "Kraken Futures"};
    for (auto exch_name : exchange_list) {
        uint8_t ex_id = refdb->get_exchange_id(exch_name);
        auto instruments = refdb->get_all_symbols_for_exchange(ex_id);
        for(auto const& instrument: instruments) {
            if(instrument->is_live || (all_instruments && (instrument->is_live < 2))){
                std::string instrument_name = std::string(instrument->instrument_name);
                if(range_given){
                    if ((instrument_name.front() < start_letter) || (instrument_name.front() > end_letter))
                        continue;
                }

                std::string cap_instrument_name = instrument_name;
                std::transform(instrument_name.begin(), instrument_name.end(), instrument_name.begin(), 
                    [](unsigned char c){ return std::tolower(c); });

                std::string file_name = "/datacollection/kraken/" + current_date + "_" + std::to_string(instrument->instrument_id) + "_ss.txt";
                FileWriter *file_writer = new FileWriter(file_name);
                auto snapinfo = new snapshot_info();
                snapinfo->file_writer = file_writer;

                std::string request_url;
                if (exch_name=="Binance") {
                    request_url = "https://api.binance.com/api/v3/depth?symbol=" + cap_instrument_name + "&limit=1000";
                } else if (exch_name=="Binance Futures") {
                    request_url = "https://fapi.binance.com/fapi/v1/depth?symbol=" + cap_instrument_name + "&limit=1000";
                } else {
                    request_url = "https://dapi.binance.com/dapi/v1/depth?symbol=" + cap_instrument_name + "&limit=1000";
                }
                snapinfo->request_url = request_url;
                snap_list.push_back(snapinfo);
            }
        }
    }

    std::thread ss_thread([snap_list]() {
        CURL * curl;
        curl = curl_easy_init();
        uint64_t timestamp;

        // Now start the loop that will snapshot every x seconds
        while(1){
            for (auto snappy : snap_list) {
                std::string response;
                curl_easy_setopt(curl, CURLOPT_URL, snappy->request_url.c_str());
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_writemethod);
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
                curl_easy_perform(curl);
                timestamp = get_current_ts();
                snappy->file_writer->write_to_file(response, timestamp);
            }
            sleep(5*60);
        }
    });
    ss_thread.detach();
}

int main(int argc, char** argv) {
    std::string_view data_view;
    WSock *wsocket;
    RefDB *refdb;
    std::string current_date;
    LogWorker *log_worker;
    Logger *logger;
    Logger *subscription_logger;

    current_date = get_current_date_as_string(0);
    bool environment_given = false;
    std::string environment_name = "";    

    bool stdout_only = false;

    bool do_collect = false;

    static struct option long_options[] = {
        {"environment"    , optional_argument, NULL, 'E'},
        {"stdout-only"    , optional_argument, NULL, 's'},
        {"offsetday"      , optional_argument, NULL, 'o'},
        {"marketdata"      , optional_argument, NULL, 'm'},
        {"all-instruments" , optional_argument, NULL, 'a'},
        {"range"           , optional_argument, NULL, 'r'},
        {"collect"        , optional_argument, NULL, 'c'},        
        {"help"           , optional_argument, NULL, 'h'}};

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
        log_worker = new LogWorker("svc_md_kraken", "All_Kraken_Exchanges", "UAT", stdout_only);
        subscription_logger = log_worker->get_new_logger("subscriptions");
        logger = log_worker->get_new_logger("base");    
        refdb = new RefDB("UAT", logger);
    } 
    else {
        log_worker = new LogWorker("svc_md_kraken", "All_Kraken_Exchanges", "PROD", stdout_only);
        subscription_logger = log_worker->get_new_logger("subscriptions");
        logger = log_worker->get_new_logger("base");
        refdb = new RefDB("PROD", logger);
    } 

    refdb->get_all_instrument_from_db();
    refdb->get_all_exchanges_from_db();

    wsocket = new WSock(logger, subscription_logger, 1800);

    // Start heartbeating
    if(do_collect){
        start_heartbeat(1, CAPTURE_SERVICE);
    } else {
        start_heartbeat(1, MARKETDATA_SERVICE);
    }

    // Start snapshot thread if collector service
    if(do_collect){
        snapshot_thread(refdb, current_date);
    }

    // Add subscriptions for both Kraken exchanges
    std::list<std::string> exchange_list {"Kraken", "Kraken Futures"};
    for (auto exch_name : exchange_list) {
        uint8_t ex_id = refdb->get_exchange_id(exch_name);
        auto instruments = refdb->get_all_symbols_for_exchange(ex_id);

        for(auto const& instrument: instruments) {
            if(instrument->is_live || (all_instruments && (instrument->is_live < 2))){
                std::string instrument_name = std::string(instrument->instrument_name);
                if(range_given){
                    if ((instrument_name.front() < start_letter) || (instrument_name.front() > end_letter))
                        continue;
                }
                std::string cap_instrument_name = instrument_name;
                std::transform(instrument_name.begin(), instrument_name.end(), instrument_name.begin(), 
                    [](unsigned char c){ return std::tolower(c); });
                logger->msg(INFO, "Adding all subscriptions for Instrument: " + instrument_name);

                std::string file_name = "/datacollection/kraken/" + current_date + "_" + std::to_string(instrument->instrument_id) + "_all.txt";

                FileWriter *file_writer;
                if(do_collect) {
                    file_writer = new FileWriter(file_name);
                } else{
                    file_writer = nullptr;
                }

                if(exch_name == "Kraken"){
                    if(do_collect){
                        wsocket->add_subscription_request("wss://ws.kraken.com/" + instrument_name + "@depth@100ms",file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    }

                    wsocket->add_subscription_request("wss://ws.kraken.com/" + instrument_name + "@trade", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://ws.kraken.com/" + instrument_name + "@bookTicker", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                }

                else if (exch_name == "Kraken Futures"){
                    if(do_collect){
                        wsocket->add_subscription_request("wss://futures.kraken.com/ws/v1/" + instrument_name + "@depth@0ms", file_writer,cap_instrument_name, instrument->instrument_id, ex_id);
                        wsocket->add_subscription_request("wss://futures.kraken.com/ws/v1/" + instrument_name + "@forceOrder", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    }

                    wsocket->add_subscription_request("wss://futures.kraken.com/ws/v1/" + instrument_name + "@trade", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://futures.kraken.com/ws/v1/" + instrument_name + "@bookTicker", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                    wsocket->add_subscription_request("wss://futures.kraken.com/ws/v1/" + instrument_name + "@markPrice@1s", file_writer, cap_instrument_name, instrument->instrument_id, ex_id);
                }
            }
        }
    }

    std::string_view message_to_print;

    // Collect the data straight to output file
    if(do_collect){
        for(;;) {
            message_to_print = wsocket->get_next_message_from_websocket();
            wsocket->get_filewriter()->write_to_file(message_to_print, wsocket->get_message_receive_time());
        }
    } 

    else {
        auto kraken_processor = new KrakenMDProcessor();
        for(;;) {
            message_to_print = wsocket->get_next_message_from_websocket();
            kraken_processor->process_message(message_to_print, wsocket->get_message_receive_time(), wsocket->get_instrument_id(), wsocket->get_exchange_id());
        }
    }

    delete(wsocket);
    return(0);
}