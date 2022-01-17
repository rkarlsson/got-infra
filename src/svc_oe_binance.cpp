#include "binance_trade_adapter.hpp"
#include <unistd.h>
#include <getopt.h>

void print_options(){
    std::cout << "Options for svc_oe_binance:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>                           = Sets to Prod or UAT config (need one of them)" << std::endl;
    std::cout << "  -l (--location) <Tokyo|London>                          = Region" << std::endl;
    std::cout << "  -e (--exchange) <Binance|Binance Futures|BinanceDEX>    = Exchange" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
 int ch;
  bool is_uat = false;
  std::string exchange = "";
  std::string location = "";
  bool environment_given = false;
  std::string environment_name = "";  
  uint8_t order_environment;
 
  static struct option long_options[] = {
    {"location"       , required_argument, NULL, 'l'},
    {"exchange"       , required_argument, NULL, 'e'},
    {"environment"    , optional_argument, NULL, 'E'},
    {"uat"            , optional_argument, NULL, 'u'},
    {"help"            , optional_argument, NULL,'h'}};

  while((ch = getopt_long(argc, argv, "E:e:l:uh", long_options, NULL)) != -1) {
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

      case 'E':
        environment_given = true;
        environment_name = optarg;
        break;

      default:
        break;
    }
  }

  if  ((environment_name != "UAT") && (environment_name != "PROD")) {
    // Need one of them provided
    // This doesn't actually do anything right now but sets the ground for the future
    print_options();
    exit(1);
  }

  if(environment_name == "UAT") {
    is_uat = true;
    order_environment = UAT_ENV;
  } else if(environment_name == "PROD") {
    is_uat = false;
    order_environment = PRODUCTION_ENV;
  } else {
    order_environment = UNKNOWN_ENV;
  }

  
  BinanceTradeAdapter b(location, order_environment);
  
  while(1){
    sleep(1);
  }
}
