#include <getopt.h>
#include "new_monitor.hpp"
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>


void print_options(){
    std::cout << "Options for svc_monitor:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>                           = Sets to Prod or UAT config (need one of them)" << std::endl;
    std::cout << "  -e (--enable) <s|i|f>                                   = Enable stdout, influx, filewriting" << std::endl;
    std::cout << "  -d (--directory) <OUTPUT DIRECTORY>                     = Sets the binary outputdirectory that svc_monitor writes to if chosen" << std::endl;    
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
  int option;
  uint8_t output_type = UNKNOWN_OUTPUT_TYPE;
  std::string outputdirectory = "/datacollection/";
  std::string environment_name = "";  
  uint8_t order_environment;

  static struct option long_options[] = {
    {"enable"       , optional_argument, NULL, 'e'},
    {"environment"  , optional_argument, NULL, 'E'},
    {"directory"         , optional_argument, NULL, 'd'},
    {"help"         , optional_argument, NULL,'h'}};

  while((option = getopt_long(argc, argv, "e:E:d:h", long_options, NULL)) != -1) {
    switch (option) {
      case 'h':
        print_options();
        exit(0);

      case 'E':
        environment_name = optarg;
        break;

      case 'd':
        outputdirectory = optarg;
        break;

      case 'e':
        switch(*optarg){
          case 's': // enable stdout
            output_type = STDOUT_OUTPUT_TYPE;
            std::cout << "Enabling stdout writing for all messages" << std::endl;
            break;
          case 'i': // enable influx writing for messages
            output_type = INFLUX_OUTPUT_TYPE;
            std::cout << "Enabling influx writing for all messages" << std::endl;
            break;
          case 'f': // enable stdout_risk
            output_type = FILE_OUTPUT_TYPE;
            std::cout << "Enabling file writing for all messages" << std::endl;
            break;              
        }
        break;

      default:
          // Do nothing - we don't accept anything else
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
    order_environment = UAT_ENV;
  } else if(environment_name == "PROD") {
    order_environment = PRODUCTION_ENV;
  } else {
    order_environment = UNKNOWN_ENV;
  }

  prometheus::Exposer exposer{"0.0.0.0:8080"};
  // create a metrics registry
  // @note it's the users responsibility to keep the object alive
  auto registry = std::make_shared<prometheus::Registry>();

  Monitor *m;
  m = new Monitor(order_environment, output_type, outputdirectory);

  while(1){
      sleep(500);
  }

  return(0);
}
