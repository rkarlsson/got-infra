#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>
#include <cstring>
#include <sstream>
#include "gzstream.hpp"
#include <list>
#include <unordered_map>
#include <getopt.h>
#include <sys/stat.h>
#include "raw_file.hpp"
#include "binary_file.hpp"
#include "aeron_types.hpp"
#include "tardis_processor.hpp"


void print_options(){
    std::cout << "Options for convert_md_tardis:" << std::endl;
    std::cout << "  -i (--inputfile) <CSVFILE_TO_PROCESS>                   = Input JSON file to process" << std::endl;
    std::cout << "  -t (--type of file) <t|T|p>                             = ToB|Trades|Pricelevel" << std::endl;    
    std::cout << "  -o (--outputfile) <OUTPUT_BINARY_FILE>                  = Output file to write the binary messages to" << std::endl;
    std::cout << "  -e (--exchange-id) <EXCHANGE_ID>                        = The ID of the exchange for the symbol" << std::endl;
    std::cout << "  -s (--symbol-id) <INSTRUMENT_ID>                        = The ID of the symbol in uint32_t" << std::endl;
    std::cout << "  -P (--price-precision) <PRICE_PRECISION>                = The price precision of the symbol in uint8_t" << std::endl;
    std::cout << "  -Q (--quantity-precision) <QUANTITY_PRECISION>          = The quantity precision of the symbol in uint8_t" << std::endl;
    std::cout << "  -T (--tick-size) <TICK_SIZE>                            = The tick_size of the symbol in double" << std::endl;
    std::cout << "  -S (--step-size) <STEP_SIZE>                            = The step_size of the symbol in double" << std::endl;
    std::cout << "  -C (--contract-size) <CONTRACT_SIZE>                    = The contract_size of the symbol in double" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

bool file_exists(const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

int main(int argc, char** argv) {
    bool csv_file_given = false;
    char csv_file_type = 't';           // t = ToB, T = Trade, P = pricelevel
    bool binary_file_given = false;
    std::string csv_filename = "";
    std::string binary_filename = "";
    BinaryFile *bin_file;
    char csv_line_buffer[1024];
    char pl_buffer[1024*1024];

    // This is used to read the csv file
    igzstream tardis_infile;
    
    static struct option long_options[] = {
        {"input-file"           , required_argument, NULL, 'i'},
        {"type-of-file"         , required_argument, NULL, 't'},
        {"output-file"          , required_argument, NULL, 'o'},
        {"exchange-id"          , required_argument, NULL, 'e'},
        {"symbol-id"            , required_argument, NULL, 's'},
        {"price-precision"      , optional_argument, NULL, 'P'},
        {"quantity-precision"   , optional_argument, NULL, 'Q'},
        {"tick-size"            , optional_argument, NULL, 'T'},
        {"step-size"            , optional_argument, NULL, 'S'},
        {"contract-size"        , optional_argument, NULL, 'C'},
        {"help"                 , optional_argument, NULL, 'h'}};

    bool exchange_id_given = false;
    bool instrument_id_given = false;

    uint32_t instrument_id = 0;
    uint32_t exchange_id = 0;
    uint32_t price_precision = 0;
    uint32_t quantity_precision = 0;
    double tick_size = 0.0;
    double step_size = 0.0;
    double contract_size = 0.0;
    double maintenance_margin = 0.0;
    double required_margin = 0.0;

    struct ToBUpdate tob_update;
    struct Trade trade_update;
    struct PLUpdates *pl_update = (PLUpdates *) pl_buffer;

    std::stringstream ss;
    ss.clear();

    int cmd_option;
    while((cmd_option = getopt_long(argc, argv, "i:t:o:e:s:P:Q:T:S:C:h", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'i':
                csv_file_given = true;
                csv_filename = optarg;
                break;

            case 't':
                csv_file_type = optarg[0];
                break;

            case 'o':
                binary_file_given = true;
                binary_filename = optarg;
                break;

            case 'e':
                exchange_id_given = true;
                ss << optarg;
                ss >> exchange_id;
                ss.clear();
                break;

            case 's':
                instrument_id_given = true;
                ss << optarg;
                ss >> instrument_id;
                ss.clear();
                break;

            case 'P':
                ss << optarg;
                ss >> price_precision;
                ss.clear();
                break;

            case 'Q':
                ss << optarg;
                ss >> quantity_precision;
                ss.clear();
                break;

            case 'T':
                ss << optarg;
                ss >> tick_size;
                ss.clear();
                break;

            case 'S':
                ss << optarg;
                ss >> step_size;
                ss.clear();
                break;

            case 'C':
                ss << optarg;
                ss >> contract_size;
                ss.clear();
                break;

            case 'h':
                print_options();
                exit(1);

            default:
                break;
        }
    }

    if(!exchange_id_given || !instrument_id_given){
        std::cout << "Need to provide exchange and instrument ID, exiting" << std::endl;
        exit(1);
    }

    if(!csv_file_given || !binary_file_given){
        std::cout << "In and out files are required as argument, exiting" << std::endl;
        exit(1);
    }

    if(file_exists(csv_filename)){
        tardis_infile.open(csv_filename.c_str());
    } else {
        std::cout << "CSV file does not exist - ending" << std::endl;
        exit(1);
    }

    bin_file = new BinaryFile(binary_filename, WriteOnlyBinary);
    bin_file->add_file_symbol_definition(   instrument_id,
                                            exchange_id,
                                            price_precision,
                                            quantity_precision,
                                            tick_size,
                                            step_size,
                                            contract_size,
                                            maintenance_margin,
                                            required_margin);
    bin_file->write_fileheader();
    
    TardisProcessor *tardis_proc = nullptr;
    char *msg_ptr = nullptr;
    uint32_t msg_len = 0;
    if(csv_file_type == 't'){ // ToB
        tardis_proc = new TardisProcessor((char *)&tob_update, csv_file_type, exchange_id, instrument_id);
        msg_ptr = (char *) &tob_update;
        msg_len = sizeof(ToBUpdate);
    }
    else if (csv_file_type == 'T'){ // Trade
        tardis_proc = new TardisProcessor((char *)&trade_update, csv_file_type, exchange_id, instrument_id);
        msg_ptr = (char *) &trade_update;
        msg_len = sizeof(Trade);
    }
    else if (csv_file_type == 'p'){ // PriceLevel
        tardis_proc = new TardisProcessor((char *)pl_update, csv_file_type, exchange_id, instrument_id);
        msg_ptr = (char *) pl_update;
        msg_len = 0;
    }

    // get first line and ignore it (csv headers)
    tardis_infile.getline(csv_line_buffer, 1023);

    // Loop over the rest of the lines in the csv file
    for(;;) {
        tardis_infile.getline(csv_line_buffer, 1023);
        if (strlen(csv_line_buffer) > 0){
            auto proc_response = tardis_proc->process_message(csv_line_buffer);
            if(csv_file_type == 'p'){
                if(proc_response){
                    // write to binary file and re_run the message parser
                    bin_file->write_message(msg_ptr, pl_update->msg_header.msgLength);
                    // return the parse
                    tardis_proc->process_message(csv_line_buffer);
                }
            }else {
                bin_file->write_message(msg_ptr, msg_len);
            }
        }
        else
            break;
        
    }
    bin_file->close();
    tardis_infile.close();
    return(0);
}