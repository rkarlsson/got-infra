#include <functional>
#include "risk_server.hpp"
#include "aeron_types.hpp"
#include "from_aeron.hpp"
#include <unistd.h>
#include <getopt.h>
#include "MyRingBuffer.hpp"


void print_options(){
    std::cout << "Options for svc_risk:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>           = Sets to Prod or UAT config (need one of them)" << std::endl;
    std::cout << "  [-h (--help)]                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
  bool environment_given = false;
  std::string environment_name = "";
  RiskServer *risk_server;

  static struct option long_options[] = {
  {"help"             , optional_argument, NULL, 'h'},
  {"environment"      , required_argument, NULL, 'E'}};
  
  int ch;
  while((ch = getopt_long(argc, argv, "hE:", long_options, NULL)) != -1) {
    std::string instrument_string;
    std::stringstream instr_stringstream;
    std::string cell;

    switch (ch) {
      case 'h':
        print_options();
        exit(0);
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

  risk_server = new RiskServer(environment_name);

  while(1) {
    sleep(1);
  }
}
