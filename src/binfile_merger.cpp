#include <cstring>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <sys/stat.h>
#include <vector>

#include "sequence_binary_files.hpp"
#include "binary_file.hpp"


bool file_exists(const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

void print_options(){
    std::cout << "Options for binfile_merger:" << std::endl;
    std::cout << "  -b (--binary_file) <binaryfilename>                     = Name of Binary File to read in" << std::endl;
    std::cout << "  -f (--outputfile) <outputfilename>                      = Name of merged output filename" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
  
    std::vector<std::string> binary_filenames;
    std::string output_filename = "";
    SequenceBinaryFiles *seq_bin_files;
  
    static struct option long_options[] = {
        {"binary_file"  , required_argument, NULL, 'b'},
        {"outputfile"   , required_argument, NULL, 'f'},
        {"help"         , optional_argument, NULL,'h'}};

    int cmd_option;
    while((cmd_option = getopt_long(argc,argv,"b:f:h", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'h':
            print_options();
            exit(0);
            break;

            case 'b':
            binary_filenames.push_back(std::string(optarg));
            break;

            case 'f':
            output_filename = std::string(optarg);
            break;
        }
    }

    if((binary_filenames.size() == 0) || (output_filename == "")){
        print_options();
        exit(0);
    }

    seq_bin_files = new SequenceBinaryFiles();
    for(auto const& bin_filename: binary_filenames) {
        if(file_exists(bin_filename))        
            seq_bin_files->add_binary_file(bin_filename, 0);
        else
            std::cout << "Filename: " << bin_filename << " does not exist" << std::endl;
    }

    uint64_t num_messages = 0;
    char *msg_ptr;
    BinaryFile *binfile_output = new BinaryFile(output_filename, WriteOnlyBinary);

    auto all_symbol_details = seq_bin_files->get_all_symbol_details();
    auto symbols = seq_bin_files->get_symbol_ids();
    for(auto const& symbol: symbols) {
        binfile_output->add_file_symbol_definition((*(SymbolDetailsMapT *)all_symbol_details)[symbol]);
    }
    binfile_output->write_fileheader();

    while(seq_bin_files->get_next_message(&msg_ptr)) {
        binfile_output->write_message(msg_ptr, ((struct MessageHeader *) msg_ptr)->msgLength);
        num_messages++;
        // switch(seq_bin_files->get_message_type()) {
        // // case TOB_UPDATE:
        // //     std::cout << "ToB Update\n";
        // //     break;

        // case TRADE:{
        //     // auto trade = (struct Trade*) msg_ptr;
        //     // std::cout << "Trade for instrument ID: " << std::to_string(trade->instrument_id) << "\n";
        //     }
        //     break;

        // // case SIGNAL:
        // //     std::cout << "Signal\n";
        // //     break;
        // }
    }
    binfile_output->close();
    std::cout << "Wrote: " << std::to_string(num_messages) << " messages" << std::endl;
}
