#include <unistd.h>
#include <getopt.h>
#include "Aeron.h"
#include <iostream>
#include "from_aeron.hpp"
#include "to_aeron.hpp"
#include "aeron_types.hpp"
#include <string>
#include <functional>

to_aeron *to_aeron_io;
bool gotten_first_quote = false;
uint32_t instrument_id_given = 0;
char side_given = 'b';
double distance_given = 1.000;
double quantity_given = 0.000;
uint8_t order_environment;

void print_options(){
    std::cout << "Options for send_oe_msg:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>                           = Sets to Prod or UAT config (need one of them)" << std::endl;
    std::cout << "  -l (--location) <Tokyo|London>                          = Region" << std::endl;
    std::cout << "  -e (--exchange) <Binance|Binance Futures|BinanceDEX>    = Exchange" << std::endl;
    std::cout << "  -i (--instrument) <instrumentid>                        = InstrumentID (default to 0)" << std::endl;
    std::cout << "  -s (--side) <b|s>                                       = Buy / Sell (default to buy)" << std::endl;
    std::cout << "  -d (--distance) <percentage away from touch>            = Percentage (default to 1.000 percent)" << std::endl;
    std::cout << "  -q (--quantity) <quantity>                              = Order quantity" << std::endl;
    std::cout << "  -o (--orderid) <orderid>                                = Order ID" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

fragment_handler_t process_io_messages() {
    return
        [&](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
            char *tmp = reinterpret_cast<char *> (buffer.buffer()) + offset;
            struct MessageHeader *m = (MessageHeader*)&tmp[0];
            
            switch(m->msgType) {
                case TOB_UPDATE: {
                    struct ToBUpdate *t = (struct ToBUpdate*)tmp;
                    if(t->instrument_id == instrument_id_given && gotten_first_quote == false){
                        gotten_first_quote = true; // process this once only
                        std::cout << "Got price update for instrument: " << std::to_string(t->instrument_id) << std::endl;
                        std::cout << "  BidPrice= " << std::to_string(static_cast<double>(t->bid_price)) << std::endl;
                        std::cout << "  BidQuantity= " << std::to_string(static_cast<double>(t->bid_qty)) << std::endl;
                        std::cout << "  AskPrice= " << std::to_string(static_cast<double>(t->ask_price)) << std::endl;
                        std::cout << "  AskQuantity= " << std::to_string(static_cast<double>(t->ask_qty)) << std::endl;

                        float order_price = 0.0;
                        if(side_given == 'b'){
                            order_price = t->ask_price * (1 - (distance_given/100));
                        } else if (side_given == 's'){
                            order_price = t->bid_price * (1 + (distance_given/100));
                        }

                        struct timespec t;
                        clock_gettime(CLOCK_REALTIME, &t);
                        struct SendOrder s;
                        // struct CancelOrder c;
                        s.msg_header.msgType = MSG_NEW_ORDER;
                        s.msg_header.msgLength = sizeof(struct SendOrder);
                        if(side_given == 'b')
                            s.is_buy     = true;
                        else
                            s.is_buy     = false;
                        s.price      = order_price;
                        s.qty        = quantity_given;
                        s.instrument_id   = instrument_id_given;
                        s.internal_order_id = order_environment;
                        s.internal_order_id <<= 56;
                        s.internal_order_id += 1 + uint32_t(t.tv_sec);
                        s.exchange_id   = 16;
                        s.strategy_id   = 1;
                        s.order_type   = LIMIT;
                        to_aeron_io->send_data((char *)&s, sizeof(s));

                        std::cout << "sent order:" << std::endl;
                        std::cout << "  orderID : " << std::to_string(s.internal_order_id) << std::endl;
                        std::cout << "  price   : " << std::to_string(s.price) << std::endl;
                        std::cout << "  qty     : " << std::to_string(s.qty) << std::endl;
                    }
                }
                break;
            }
        };
}

int main(int argc, char* argv[]) {
    int ch;
    // bool is_uat = false;
    std::string exchange = "";
    std::string location = "";
    // bool environment_given = false;
    uint64_t order_id;
    bool order_id_given = false;
    std::string environment_name = "PROD";


    std::function<fragment_handler_t()> fragment_handler;
    from_aeron *from_aeron_io;

    static struct option long_options[] = {
        {"exchange"       , required_argument, NULL, 'e'},
        {"instrument"     , required_argument, NULL, 'i'},
        {"side"           , required_argument, NULL, 's'},
        {"distance"       , required_argument, NULL, 'd'},
        {"orderid"        , optional_argument, NULL, 'o'},
        {"environment"    , optional_argument, NULL, 'E'},
        {"help"           , optional_argument, NULL,'h'}};

    while((ch = getopt_long(argc, argv, "E:e:i:s:d:q:o:h", long_options, NULL)) != -1) {
        std::string instrument_string;
        std::stringstream instr_stringstream;
        std::string cell;

        switch (ch) {
            case 'h':
                print_options();
                exit(0);

            case 'e':
            exchange = optarg;
            break;

            case 'l':
            location = optarg;
            break;

            case 'o':
            order_id = std::stoull(optarg);
            order_id_given = true;
            break;

            case 'E':
            // environment_given = true;
            environment_name = optarg;
            break;

            case 'i':
            instrument_id_given = std::stoi(optarg);
            break;

            case 's':
            if (optarg[0] == 's'){
                side_given = 's';
            } else if (optarg[0] == 'b'){
                side_given = 'b';
            } else {
                std::cout << "Unknown side value given - exiting.." << std::endl;
                exit(0);
            }
            break;

            case 'd':
            distance_given = std::stod(optarg);
            break;

            case 'q':
            quantity_given = std::stod(optarg);
            break;

            default:
            break;
        }
    }

    if  (((environment_name != "UAT") && (environment_name != "PROD")) ||
        (instrument_id_given == 0)) {
        // Need one of them provided
        // This doesn't actually do anything right now but sets the ground for the future
        print_options();
        exit(1);
    }

    if(environment_name == "UAT") {
        // is_uat = true;
        order_environment = UAT_ENV;
    } else if(environment_name == "PROD") {
        // is_uat = false;
        order_environment = PRODUCTION_ENV;
    } else {
        order_environment = UNKNOWN_ENV;
    }

    to_aeron_io = new to_aeron(AERON_IO);

    if(order_id_given) {
        // SENDING CANCEL REQ
        struct CancelOrder cancel_message;
        cancel_message.msg_header.msgType = MSG_CANCEL_ORDER;
        cancel_message.msg_header.msgLength = sizeof(struct CancelOrder);
        cancel_message.msg_header.protoVersion = 1;
        cancel_message.exchange_id = 16;
        cancel_message.cancel_type = NORMAL_CANCEL;
        cancel_message.internal_order_id = order_id;
        cancel_message.instrument_id = instrument_id_given;
        cancel_message.strategy_id = 0;
        to_aeron_io->send_data((char *)&cancel_message, sizeof(struct CancelOrder));
    } else {
        fragment_handler = std::bind(&process_io_messages);
        from_aeron_io = new from_aeron(AERON_IO, fragment_handler);

        do{
            sleep(1);
        }while(gotten_first_quote == false);
    }

}
