#include <cstring>
#include <iostream>
#include <unistd.h>
#include <getopt.h>
#include <sys/stat.h>
#include <vector>

#include "sequence_binary_files.hpp"
#include "binary_file.hpp"
#include "merged_orderbook.hpp"


bool file_exists(const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

void print_options(){
    std::cout << "Options for binfile_merger:" << std::endl;
    std::cout << "  -b (--binary_file) <binaryfilename>                     = Name of Binary File to read in" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
  
    std::vector<std::string> binary_filenames;
    SequenceBinaryFiles *seq_bin_files;
 
    static struct option long_options[] = {
        {"binary_file"  , required_argument, NULL, 'b'},
        {"help"         , optional_argument, NULL,'h'}};

    int cmd_option;
    while((cmd_option = getopt_long(argc,argv,"b:h", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'h':
            print_options();
            exit(0);
            break;

            case 'b':
            binary_filenames.push_back(std::string(optarg));
            break;
        }
    }

    if(binary_filenames.size() == 0){
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

    struct ToBUpdate *tob;
    double tob_bid = 0.0;
    double tob_ask = 0.0;
    struct PLUpdates *plupdate;
    MergedOrderbook pl_book;
    char *msg_ptr;

    while(seq_bin_files->get_next_message(&msg_ptr)) {
        struct MessageHeader *msg = (struct MessageHeader *) msg_ptr;
        switch(msg->msgType){
            case TOB_UPDATE:
                tob = (struct ToBUpdate*)msg_ptr;
                tob_bid = tob->bid_price;
                tob_ask = tob->ask_price;
                break;

            case PL_UPDATE:
                plupdate = (struct PLUpdates*)msg_ptr;
                pl_book.process_update(plupdate);
                std::cout << "(" << std::to_string(pl_book.get_touch_price_ask()) << " - " << std::to_string(tob_ask) << ")     :     (";
                std::cout << std::to_string(pl_book.get_touch_price_bid()) << " - " << std::to_string(tob_bid) << ")" << std::endl;
                break;

            case INSTRUMENT_CLEAR_BOOK:
                pl_book.clear_orderbook();
                break;
        }
    }
}