#include "Aeron.h"
#include <unistd.h>
#include <getopt.h>
#include <cstdlib>
#include <vector>
#include "to_aeron.hpp"
#include "aeron_types.hpp"
#include "refdata_lib.h"


void print_options(){
    std::cout << "Options for msg_sender:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>                   = Sets to Prod or UAT config (need one of them)" << std::endl;    
    std::cout << "  [-a (--account-info) <EXCHANGE_NAME>]           = Triggers account update request for specified exchange" << std::endl;
    std::cout << "  [-t (--account-info-type) <FUNDING|POSITION>]   = Either get Funding fee updates or Position updates" << std::endl;
    std::cout << "  [-s (--strategy-info)]                          = Triggers risk reload in linehandlers" << std::endl;
    std::cout << "  [-m (--margin) <TRANSFER|BORROW|WALLET|INFO>]   = Sends a margin request to the linehandler" << std::endl;
    std::cout << "  [-r (--rollover)]                               = Triggers the database rollover" << std::endl;
    std::cout << "  [-i (--instrument-update)]                      = Triggers a refdata query and update for all linehandlers" << std::endl;
    std::cout << "  [-e (--early)]                                  = In combination with \"-r\" if run rollover before midnight" << std::endl;
    std::cout << "  [-h (--help)]                                   = Prints this message" << std::endl;    
}


int main(int argc, char* argv[]) {
    int ch;    
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    uint64_t recv_ts = (t.tv_sec*1000000000L)+t.tv_nsec;
    struct RolloverRequest rollover_message;
    std::string exchange_name = "";
    
    // This ironically seems to be needed for aeron to work headless from crontab
    setenv("USER", "ubuntu", true);

    bool rollover = false;
    bool strategy_info = false;
    bool early = false;
    bool account_info = false;
    bool account_info_type = false;

    bool margin_info = false;
    bool margin_info_wallet = false;
    bool margin_transfer = false;
    bool margin_borrow = false;
    bool instrument_update = false;

    bool environment_given = false;
    std::string environment_name = "";  
    uint8_t order_environment;      
    uint8_t update_type;
  
    uint64_t extra_day = 24*60*60*1000000000L;

    static struct option long_options[] = {
        {"help"       , optional_argument, NULL, 'h'},
        {"account-info" , optional_argument, NULL, 'a'},
        {"account-info-type" , optional_argument, NULL, 't'},
        {"rollover"       , optional_argument, NULL, 'r'},
        {"instrument-update" , optional_argument, NULL, 'i'},
        {"early"       , optional_argument, NULL, 'e'},
        {"margin"    , optional_argument, NULL, 'm'},
        {"environment"    , optional_argument, NULL, 'E'},
        {"strategy-info"       , optional_argument, NULL, 's'}};

    while((ch = getopt_long(argc, argv, "ihresa:t:E:m:", long_options, NULL)) != -1) {
        switch (ch) {
            case 'h':
                print_options();
                exit(0);

            case 'E':
                environment_given = true;
                environment_name = optarg;
                break;

            case 'm':{
                std::string request_type = optarg;
                if(request_type == "INFO") {
                    margin_info = true;
                } else if (request_type == "WALLET") {
                    margin_info_wallet = true;
                } else if (request_type == "TRANSFER") {
                    margin_transfer = true;
                } else if (request_type == "BORROW") {
                    margin_borrow = true;
                } else {
                    std::cout << "Provide a valid margin request type, exiting." << std::endl;
                    exit(0);
                }
                }
                break;

            case 'r':
                rollover = true;
                break;

            case 'i':
                instrument_update = true;
                break;

            case 'a':
                exchange_name = optarg;
                account_info = true;
                break;

            case 't':
                account_info_type = true;{
                    std::string temp_string = optarg;
                    if (temp_string == "FUNDING") {
                        update_type = FUNDING_FEE;
                    } else if (temp_string == "POSITION") {
                        update_type = POSITION_RISK_REQUEST;
                    } else {
                        account_info_type = false;
                        // set to false and basically fail request
                    }
                }
                break;

            case 'e':
                early = true;
                break;

            case 's':
                strategy_info = true;
                break;

            default:
                break;
        }
    }

    // Do nothing if no option has been given
    if(argc < 2){
        print_options();
        exit(0);
    }

    // Lets not create these before here so we don't initialise them for help message
    to_aeron *to_aeron_io;
    to_aeron_io = new to_aeron(AERON_IO);

    if(rollover)
    {
        //Lets run rollver
        rollover_message.msg_header.msgType = ROLL_OVER_REQUEST;
        rollover_message.msg_header.msgLength = sizeof(struct RolloverRequest);
        rollover_message.msg_header.protoVersion = 1;
        if(early){
            rollover_message.request_time_stamp = recv_ts + extra_day;
        } else {
            rollover_message.request_time_stamp = recv_ts;
        }
        to_aeron_io->send_data((char *)&rollover_message, sizeof(rollover_message));
    }

    if(strategy_info) {
        // This will trigger strategyinfo renewal in exchange linehandlers        
        StrategyInfoRequest strategy_request = {MessageHeaderT{sizeof(StrategyInfoRequest), STRATEGY_INFO_REQUEST, 0}, 0};
        to_aeron_io->send_data((char *)&strategy_request, sizeof(strategy_request )); 
    }

    if(account_info) {
        if (! account_info_type) {
            std::cout << "Not set account info type.. exiting.." << std::endl;
            exit(1);
        }

        std::cout << "Getting all exchange details from the refdatabase" << std::endl;
        std::vector<exchange_info> exchanges;
        get_all_exchanges_from_db(exchanges);
        bool found_exchange = false;

        for(auto const& exchange: exchanges) {
            if(exchange.exchange_name == exchange_name) {
               AccountInfoRequest acct_request = {MessageHeaderT{sizeof(AccountInfoRequest), ACCOUNT_INFO_REQUEST, 0}, exchange.exchange_id, update_type};
               to_aeron_io->send_data((char *)&acct_request, sizeof(acct_request)); 
               std::cout << "Found exchange_id, sending the account info request" << std::endl;
               found_exchange = true;
            }
        }
        if(! found_exchange)
            std::cout << "Couldn't find exchange_id from name, failed to send" << std::endl;
    }

    if(margin_info || margin_info_wallet) {
        uint8_t info_type;
        if(margin_info){
            info_type = CROSS_COLLATERAL_INFO;
        }
        else if (margin_info_wallet) {
            info_type = CROSS_COLLATERAL_WALLET;
        }

        // Statically set this to exchange ID Binance - should change this
        MarginInfoRequest margin_request = {MessageHeaderT{sizeof(MarginInfoRequest), MARGIN_INFO_REQUEST, 0}, 0, 16, info_type};
        to_aeron_io->send_data((char *)&margin_request, sizeof(margin_request)); 
    }

    if(margin_transfer) {
        MarginTransferRequest margin_request = {MessageHeaderT{sizeof(MarginTransferRequest), MARGIN_TRANSFER_REQUEST, 0}, 12.00, 1, 3722, 1, 0, 16};
        to_aeron_io->send_data((char *)&margin_request, sizeof(margin_request)); 
    }

    if(margin_borrow) {
        MarginBorrowRequest margin_request = {MessageHeaderT{sizeof(MarginBorrowRequest), MARGIN_BORROW_REQUEST, 0}, 12.00, 3722, 12.00, 571, 1, 1, 16};
        to_aeron_io->send_data((char *)&margin_request, sizeof(margin_request)); 
    }

    if(instrument_update) {
        InstrumentInfoRequest instrument_request = {MessageHeaderT{sizeof(InstrumentInfoRequest), INSTRUMENT_INFO_REQUEST, 0}, 0};
        to_aeron_io->send_data((char *)&instrument_request, sizeof(instrument_request)); 
    }

}
