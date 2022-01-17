#include <cstring>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <sys/stat.h>
#include <functional>
#include <vector>

#include "Aeron.h"
#include "from_aeron.hpp"
#include "aeron_types.hpp"
#include "sequence_binary_files.hpp"

// Signals
#include "test_signal.hpp"

bool file_exists(const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

void print_options(){
    std::cout << "Options for svc_sig_generator:" << std::endl;
    std::cout << "  -i (--input) <AERON|FILE>                               = Input source - aeron bus for prod or binary files for offline generation/testing" << std::endl;
    std::cout << "  -o (--output) <AERON|FILE>                              = Ouput format - aeron bus for prod or binary files for testing/confirmation" << std::endl;
    std::cout << "  -s (--signalname) <TESTSIG|OTHERSIG>                    = Name of signal to run" << std::endl;
    std::cout << "  [-b (--binary_file) <binaryinputfilename>]              = Name of Binary File to read in" << std::endl;
    std::cout << "  [-f (--outputfile) <binaryoutputfilename>]              = Name of generated signal output binary filename" << std::endl;  
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
  
    std::vector<std::string> binary_filenames;
    std::string output_filename = "";
    SequenceBinaryFiles *seq_bin_files = nullptr;

    // Aeron stuff

    // default to files as input and output
    bool file_input = true;
    bool file_output = true;

    // Signalname vars
    std::string signal_name = "";
  
    static struct option long_options[] = {
        {"input"        , required_argument, NULL, 'i'},
        {"output"       , required_argument, NULL, 'o'},
        {"signalname"   , required_argument, NULL, 's'},
        {"binary_file"  , optional_argument, NULL, 'b'},
        {"outputfile"   , optional_argument, NULL, 'f'},
        {"help"         , optional_argument, NULL,'h'}};

    int cmd_option;
    while((cmd_option = getopt_long(argc,argv,"i:o:s:b:f:h", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'h':
            print_options();
            exit(0);
            break;

            case 'i':
            if(std::string(optarg) == "AERON")
                file_input = false;
            break;

            case 'o':
            if(std::string(optarg) == "AERON")
                file_output = false;
            break;

            case 's':
            signal_name = std::string(optarg);
            break;

            case 'b':
            binary_filenames.push_back(std::string(optarg));
            break;

            case 'f':
            output_filename = std::string(optarg);
            break;
        }
    }

    if(signal_name == ""){
        std::cout << "You need to provide a signalname to the argument parameters" << std::endl;
        exit(0);
    }

    if(file_output){
        if(output_filename == "") {
            std::cout << "Need an output filename for the generated binary file" << std::endl;
            exit(0);
        }
    }

    if(file_input){
        if(binary_filenames.size() == 0) {
            std::cout << "No binary files given as input but you have indicated to use binary files as input (default)" << std::endl;
            exit(0);
        } else {
            // initiate the sequenced input files
            seq_bin_files = new SequenceBinaryFiles();
            for(auto const& bin_filename: binary_filenames) {
                if(file_exists(bin_filename))        
                    seq_bin_files->add_binary_file(bin_filename, 0);
                else
                    std::cout << "Filename: " << bin_filename << " does not exist" << std::endl;
            }
        }
    }

    if(signal_name == "TESTSIG"){
        auto signal = new TestSignal(file_output, output_filename, file_input, seq_bin_files);
        signal->run();
    }


}
